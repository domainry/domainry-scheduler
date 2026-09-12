package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/domainry/domainry-orm/query"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulerpersistence "github.com/domainry/domainry-scheduler-sdk/persistence"
)

type ScheduledPlanStore struct {
	database  modulehost.Database
	dialect   modulehost.Dialect
	runtimeID string
}

func NewScheduledPlanStore(database modulehost.Database, dialect modulehost.Dialect, runtimeID string) (*ScheduledPlanStore, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if database == nil || dialect == nil || runtimeID == "" {
		return nil, fmt.Errorf("Scheduler plan store database, dialect, and Runtime identity are required")
	}
	return &ScheduledPlanStore{database: database, dialect: dialect, runtimeID: runtimeID}, nil
}

func (s *ScheduledPlanStore) CreateScheduledPlan(ctx context.Context, record schedulerpersistence.ScheduledPlanRecord) (schedulersdk.ScheduledPlan, bool, error) {
	plan := record.Plan
	triggerJSON, err := json.Marshal(plan.Trigger)
	if err != nil {
		return schedulersdk.ScheduledPlan{}, false, err
	}
	actionsJSON, err := json.Marshal(plan.AllowedActions)
	if err != nil {
		return schedulersdk.ScheduledPlan{}, false, err
	}
	targetJSON, err := json.Marshal(plan.Target)
	if err != nil {
		return schedulersdk.ScheduledPlan{}, false, err
	}
	statement, args, err := query.NewInsertBuilder(s.dialect, "_scheduler_plans").Columns(
		"runtime_id", "plan_id", "client_id", "request_sha256", "workspace_id", "user_id", "product_key", "name", "timezone",
		"trigger_json", "input_json", "allowed_actions_json", "target_json", "source_conversation_id", "source_run_id", "status", "revision", "created_at", "updated_at",
	).Values(
		s.runtimeID, plan.ID, record.ClientID, record.RequestSHA256, plan.Owner.WorkspaceID, plan.Owner.UserID, plan.Owner.ProductKey, plan.Name, plan.Timezone,
		string(triggerJSON), string(plan.Input), string(actionsJSON), string(targetJSON), nullable(plan.ConversationRef.ConversationID), nullable(plan.ConversationRef.RunID),
		plan.Status, plan.Revision, formatTime(plan.CreatedAt), formatTime(plan.UpdatedAt),
	).OnConflictDoNothing("runtime_id", "plan_id").Build()
	if err != nil {
		return schedulersdk.ScheduledPlan{}, false, err
	}
	result, err := s.database.ExecContext(ctx, statement, args...)
	if err != nil {
		if !isUnique(err) {
			return schedulersdk.ScheduledPlan{}, false, err
		}
	}
	affected := int64(0)
	if err == nil {
		affected, _ = result.RowsAffected()
	}
	stored, clientID, requestSHA256, getErr := s.getScheduledPlan(ctx, plan.Owner, plan.ID)
	if getErr != nil {
		return schedulersdk.ScheduledPlan{}, false, getErr
	}
	if clientID != record.ClientID || requestSHA256 != record.RequestSHA256 {
		return schedulersdk.ScheduledPlan{}, false, schedulersdk.ErrScheduledPlanConflict
	}
	return stored, affected == 0, nil
}

func (s *ScheduledPlanStore) GetScheduledPlan(ctx context.Context, lookup schedulersdk.ScheduledPlanLookup) (schedulersdk.ScheduledPlan, error) {
	plan, _, _, err := s.getScheduledPlan(ctx, lookup.Owner, strings.TrimSpace(lookup.PlanID))
	return plan, err
}

func (s *ScheduledPlanStore) ListScheduledPlans(ctx context.Context, input schedulersdk.ScheduledPlanList) (schedulersdk.ScheduledPlanPage, error) {
	limit := input.Limit
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	predicates := []query.Predicate{
		query.Equal("runtime_id", s.runtimeID),
		query.Equal("workspace_id", input.Owner.WorkspaceID),
		query.Equal("user_id", input.Owner.UserID),
		query.Equal("product_key", input.Owner.ProductKey),
		query.NotEqual("status", schedulersdk.ScheduledPlanStatusDeleted),
	}
	if input.Status != "" {
		predicates = append(predicates, query.Equal("status", input.Status))
	}
	if input.Cursor != "" {
		predicates = append(predicates, query.GreaterThan("plan_id", input.Cursor))
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_scheduler_plans").Columns("plan_id").
		Where(query.And(predicates...)).OrderBy(query.Ascending("plan_id")).Limit(limit + 1).Build()
	if err != nil {
		return schedulersdk.ScheduledPlanPage{}, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return schedulersdk.ScheduledPlanPage{}, err
	}
	defer rows.Close()
	ids := make([]string, 0, limit+1)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return schedulersdk.ScheduledPlanPage{}, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return schedulersdk.ScheduledPlanPage{}, err
	}
	page := schedulersdk.ScheduledPlanPage{Items: make([]schedulersdk.ScheduledPlan, 0, min(len(ids), limit))}
	if len(ids) > limit {
		ids = ids[:limit]
		page.NextCursor = ids[len(ids)-1]
	}
	for _, id := range ids {
		plan, _, _, err := s.getScheduledPlan(ctx, input.Owner, id)
		if err != nil {
			return schedulersdk.ScheduledPlanPage{}, err
		}
		page.Items = append(page.Items, plan)
	}
	return page, nil
}

// ReplaceScheduledPlan performs the complete mutable-state write with a
// revision predicate. The plan owner and ID are in the predicate, so a caller
// cannot turn a successful CAS into a cross-owner move.
func (s *ScheduledPlanStore) ReplaceScheduledPlan(ctx context.Context, plan schedulersdk.ScheduledPlan, expectedRevision int64) (schedulersdk.ScheduledPlan, error) {
	triggerJSON, actionsJSON, targetJSON, err := scheduledPlanJSON(plan)
	if err != nil {
		return schedulersdk.ScheduledPlan{}, err
	}
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_scheduler_plans").
		Set("name", plan.Name).
		Set("timezone", plan.Timezone).
		Set("trigger_json", triggerJSON).
		Set("input_json", string(plan.Input)).
		Set("allowed_actions_json", actionsJSON).
		Set("target_json", targetJSON).
		Set("source_conversation_id", nullable(plan.ConversationRef.ConversationID)).
		Set("source_run_id", nullable(plan.ConversationRef.RunID)).
		Set("status", plan.Status).
		Set("revision", plan.Revision).
		Set("updated_at", formatTime(plan.UpdatedAt)).
		Where(query.And(
			query.Equal("runtime_id", s.runtimeID), query.Equal("plan_id", plan.ID),
			query.Equal("workspace_id", plan.Owner.WorkspaceID), query.Equal("user_id", plan.Owner.UserID),
			query.Equal("product_key", plan.Owner.ProductKey), query.Equal("revision", expectedRevision),
		)).Build()
	if err != nil {
		return schedulersdk.ScheduledPlan{}, err
	}
	result, err := s.database.ExecContext(ctx, statement, args...)
	if err != nil {
		return schedulersdk.ScheduledPlan{}, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return schedulersdk.ScheduledPlan{}, err
	}
	if affected != 1 {
		if _, _, _, getErr := s.getScheduledPlan(ctx, plan.Owner, plan.ID); getErr != nil {
			return schedulersdk.ScheduledPlan{}, getErr
		}
		return schedulersdk.ScheduledPlan{}, schedulersdk.ErrScheduledPlanConflict
	}
	stored, _, _, err := s.getScheduledPlan(ctx, plan.Owner, plan.ID)
	return stored, err
}

func (s *ScheduledPlanStore) ListScheduledPlansForRecovery(ctx context.Context, after string, limit int) ([]schedulersdk.ScheduledPlan, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	predicate := query.Predicate(query.Equal("runtime_id", s.runtimeID))
	if after = strings.TrimSpace(after); after != "" {
		predicate = query.And(predicate, query.GreaterThan("plan_id", after))
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_scheduler_plans").Columns(
		"plan_id", "workspace_id", "user_id", "product_key",
	).Where(predicate).OrderBy(query.Ascending("plan_id")).Limit(limit).Build()
	if err != nil {
		return nil, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type identity struct {
		id    string
		owner schedulersdk.ScheduledPlanOwner
	}
	var identities []identity
	for rows.Next() {
		var item identity
		if err := rows.Scan(&item.id, &item.owner.WorkspaceID, &item.owner.UserID, &item.owner.ProductKey); err != nil {
			return nil, err
		}
		identities = append(identities, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	plans := make([]schedulersdk.ScheduledPlan, 0, len(identities))
	for _, item := range identities {
		plan, _, _, err := s.getScheduledPlan(ctx, item.owner, item.id)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

func (s *ScheduledPlanStore) getScheduledPlan(ctx context.Context, owner schedulersdk.ScheduledPlanOwner, planID string) (schedulersdk.ScheduledPlan, string, string, error) {
	statement, args, err := query.NewSelectBuilder(s.dialect, "_scheduler_plans").Columns(
		"client_id", "request_sha256", "name", "timezone", "trigger_json", "input_json", "allowed_actions_json", "target_json",
		"source_conversation_id", "source_run_id", "status", "revision", "created_at", "updated_at",
	).Where(query.And(
		query.Equal("runtime_id", s.runtimeID), query.Equal("plan_id", planID), query.Equal("workspace_id", owner.WorkspaceID),
		query.Equal("user_id", owner.UserID), query.Equal("product_key", owner.ProductKey),
	)).Build()
	if err != nil {
		return schedulersdk.ScheduledPlan{}, "", "", err
	}
	var clientID, requestSHA256, name, timezone, triggerJSON, inputJSON, actionsJSON, targetJSON, status, createdAt, updatedAt string
	var conversationID, runID sql.NullString
	var revision int64
	if err := s.database.QueryRowContext(ctx, statement, args...).Scan(
		&clientID, &requestSHA256, &name, &timezone, &triggerJSON, &inputJSON, &actionsJSON, &targetJSON,
		&conversationID, &runID, &status, &revision, &createdAt, &updatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return schedulersdk.ScheduledPlan{}, "", "", schedulersdk.ErrScheduledPlanNotFound
		}
		return schedulersdk.ScheduledPlan{}, "", "", err
	}
	plan := schedulersdk.ScheduledPlan{
		ID: planID, Name: name, Owner: owner, Timezone: timezone, Input: json.RawMessage(inputJSON),
		ConversationRef: schedulersdk.ScheduledPlanConversationRef{ConversationID: conversationID.String, RunID: runID.String},
		Status:          status, Revision: revision, CreatedAt: parseTime(createdAt), UpdatedAt: parseTime(updatedAt),
	}
	if err := json.Unmarshal([]byte(triggerJSON), &plan.Trigger); err != nil {
		return schedulersdk.ScheduledPlan{}, "", "", err
	}
	if err := json.Unmarshal([]byte(actionsJSON), &plan.AllowedActions); err != nil {
		return schedulersdk.ScheduledPlan{}, "", "", err
	}
	if err := json.Unmarshal([]byte(targetJSON), &plan.Target); err != nil {
		return schedulersdk.ScheduledPlan{}, "", "", err
	}
	return plan, clientID, requestSHA256, nil
}

func scheduledPlanJSON(plan schedulersdk.ScheduledPlan) (string, string, string, error) {
	trigger, err := json.Marshal(plan.Trigger)
	if err != nil {
		return "", "", "", err
	}
	actions, err := json.Marshal(plan.AllowedActions)
	if err != nil {
		return "", "", "", err
	}
	target, err := json.Marshal(plan.Target)
	if err != nil {
		return "", "", "", err
	}
	return string(trigger), string(actions), string(target), nil
}

var _ schedulerpersistence.ScheduledPlanRepository = (*ScheduledPlanStore)(nil)
var _ schedulerpersistence.ScheduledPlanRecoveryRepository = (*ScheduledPlanStore)(nil)
