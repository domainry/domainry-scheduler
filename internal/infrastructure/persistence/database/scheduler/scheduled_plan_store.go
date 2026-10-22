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

const scheduledPlansTable = "_scheduler_plans"

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
	payload, err := json.Marshal(plan)
	if err != nil {
		return schedulersdk.ScheduledPlan{}, false, err
	}
	statement, args, err := query.NewInsertBuilder(s.dialect, scheduledPlansTable).Columns(
		"plan_id", "runtime_id", "client_id", "request_sha256", "workspace_id", "user_id", "product_key", "status", "plan_revision", "plan_json", "created_at", "updated_at",
	).Values(
		plan.PlanID, s.runtimeID, record.ClientID, record.RequestSHA256, plan.Owner.WorkspaceID, plan.Owner.UserID, plan.Owner.ProductKey,
		plan.Status, plan.Revision, string(payload), formatTime(plan.CreatedAt), formatTime(plan.UpdatedAt),
	).OnConflictDoNothing("runtime_id", "workspace_id", "user_id", "product_key", "client_id").Build()
	if err != nil {
		return schedulersdk.ScheduledPlan{}, false, err
	}
	result, err := s.database.ExecContext(ctx, statement, args...)
	if err != nil && !isUnique(err) {
		return schedulersdk.ScheduledPlan{}, false, err
	}
	affected := int64(0)
	if err == nil {
		affected, _ = result.RowsAffected()
	}
	if affected == 1 {
		stored, _, _, getErr := s.getScheduledPlan(ctx, plan.Owner, plan.PlanID)
		return stored, false, getErr
	}
	stored, clientID, requestSHA256, getErr := s.getScheduledPlanByClient(ctx, plan.Owner, record.ClientID)
	if getErr != nil {
		if errors.Is(getErr, schedulersdk.ErrScheduledPlanNotFound) {
			return schedulersdk.ScheduledPlan{}, false, schedulersdk.ErrScheduledPlanConflict
		}
		return schedulersdk.ScheduledPlan{}, false, getErr
	}
	if clientID != record.ClientID || requestSHA256 != record.RequestSHA256 {
		return schedulersdk.ScheduledPlan{}, false, schedulersdk.ErrScheduledPlanConflict
	}
	return stored, true, nil
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
		query.Equal("runtime_id", s.runtimeID), query.Equal("workspace_id", input.Owner.WorkspaceID), query.Equal("user_id", input.Owner.UserID),
		query.Equal("product_key", input.Owner.ProductKey), query.NotEqual("status", schedulersdk.ScheduledPlanStatusDeleted),
	}
	if input.Status != "" {
		predicates = append(predicates, query.Equal("status", input.Status))
	}
	if input.Cursor > 0 {
		predicates = append(predicates, query.GreaterThan("id", input.Cursor))
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, scheduledPlansTable).Columns("id", "plan_id").
		Where(query.And(predicates...)).OrderBy(query.Ascending("id")).Limit(limit + 1).Build()
	if err != nil {
		return schedulersdk.ScheduledPlanPage{}, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return schedulersdk.ScheduledPlanPage{}, err
	}
	defer rows.Close()
	type identity struct {
		id     int64
		planID string
	}
	identities := make([]identity, 0, limit+1)
	for rows.Next() {
		var item identity
		if err := rows.Scan(&item.id, &item.planID); err != nil {
			return schedulersdk.ScheduledPlanPage{}, err
		}
		identities = append(identities, item)
	}
	if err := rows.Err(); err != nil {
		return schedulersdk.ScheduledPlanPage{}, err
	}
	page := schedulersdk.ScheduledPlanPage{Items: make([]schedulersdk.ScheduledPlan, 0, min(len(identities), limit))}
	if len(identities) > limit {
		identities = identities[:limit]
		page.NextCursor = identities[len(identities)-1].id
	}
	for _, item := range identities {
		plan, _, _, err := s.getScheduledPlan(ctx, input.Owner, item.planID)
		if err != nil {
			return schedulersdk.ScheduledPlanPage{}, err
		}
		page.Items = append(page.Items, plan)
	}
	return page, nil
}

func (s *ScheduledPlanStore) ReplaceScheduledPlan(ctx context.Context, plan schedulersdk.ScheduledPlan, expectedRevision int64) (schedulersdk.ScheduledPlan, error) {
	payload, err := json.Marshal(plan)
	if err != nil {
		return schedulersdk.ScheduledPlan{}, err
	}
	statement, args, err := query.NewUpdateBuilder(s.dialect, scheduledPlansTable).
		Set("status", plan.Status).Set("plan_revision", plan.Revision).Set("plan_json", string(payload)).Set("updated_at", formatTime(plan.UpdatedAt)).
		Where(query.And(
			query.Equal("runtime_id", s.runtimeID), query.Equal("plan_id", plan.PlanID), query.Equal("workspace_id", plan.Owner.WorkspaceID),
			query.Equal("user_id", plan.Owner.UserID), query.Equal("product_key", plan.Owner.ProductKey), query.Equal("plan_revision", expectedRevision),
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
		if _, _, _, getErr := s.getScheduledPlan(ctx, plan.Owner, plan.PlanID); getErr != nil {
			return schedulersdk.ScheduledPlan{}, getErr
		}
		return schedulersdk.ScheduledPlan{}, schedulersdk.ErrScheduledPlanConflict
	}
	stored, _, _, err := s.getScheduledPlan(ctx, plan.Owner, plan.PlanID)
	return stored, err
}

func (s *ScheduledPlanStore) ListScheduledPlansForRecovery(ctx context.Context, after int64, limit int) ([]schedulersdk.ScheduledPlan, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	predicate := query.Predicate(query.Equal("runtime_id", s.runtimeID))
	if after > 0 {
		predicate = query.And(predicate, query.GreaterThan("id", after))
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, scheduledPlansTable).Columns("id", "plan_id", "workspace_id", "user_id", "product_key").
		Where(predicate).OrderBy(query.Ascending("id")).Limit(limit).Build()
	if err != nil {
		return nil, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type identity struct {
		id     int64
		planID string
		owner  schedulersdk.ScheduledPlanOwner
	}
	var identities []identity
	for rows.Next() {
		var item identity
		if err := rows.Scan(&item.id, &item.planID, &item.owner.WorkspaceID, &item.owner.UserID, &item.owner.ProductKey); err != nil {
			return nil, err
		}
		identities = append(identities, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	plans := make([]schedulersdk.ScheduledPlan, 0, len(identities))
	for _, item := range identities {
		plan, _, _, err := s.getScheduledPlan(ctx, item.owner, item.planID)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

func (s *ScheduledPlanStore) getScheduledPlan(ctx context.Context, owner schedulersdk.ScheduledPlanOwner, planID string) (schedulersdk.ScheduledPlan, string, string, error) {
	predicate := query.And(
		query.Equal("runtime_id", s.runtimeID), query.Equal("plan_id", planID), query.Equal("workspace_id", owner.WorkspaceID),
		query.Equal("user_id", owner.UserID), query.Equal("product_key", owner.ProductKey),
	)
	return s.loadScheduledPlan(ctx, predicate, owner, planID)
}

func (s *ScheduledPlanStore) getScheduledPlanByClient(ctx context.Context, owner schedulersdk.ScheduledPlanOwner, clientID string) (schedulersdk.ScheduledPlan, string, string, error) {
	predicate := query.And(
		query.Equal("runtime_id", s.runtimeID), query.Equal("client_id", clientID), query.Equal("workspace_id", owner.WorkspaceID),
		query.Equal("user_id", owner.UserID), query.Equal("product_key", owner.ProductKey),
	)
	return s.loadScheduledPlan(ctx, predicate, owner, "")
}

func (s *ScheduledPlanStore) loadScheduledPlan(ctx context.Context, predicate query.Predicate, owner schedulersdk.ScheduledPlanOwner, expectedPlanID string) (schedulersdk.ScheduledPlan, string, string, error) {
	statement, args, err := query.NewSelectBuilder(s.dialect, scheduledPlansTable).Columns("id", "plan_id", "client_id", "request_sha256", "plan_json").Where(predicate).Build()
	if err != nil {
		return schedulersdk.ScheduledPlan{}, "", "", err
	}
	var physicalID int64
	var planID, clientID, requestSHA256, payload string
	if err := s.database.QueryRowContext(ctx, statement, args...).Scan(&physicalID, &planID, &clientID, &requestSHA256, &payload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return schedulersdk.ScheduledPlan{}, "", "", schedulersdk.ErrScheduledPlanNotFound
		}
		return schedulersdk.ScheduledPlan{}, "", "", err
	}
	var plan schedulersdk.ScheduledPlan
	if err := json.Unmarshal([]byte(payload), &plan); err != nil {
		return schedulersdk.ScheduledPlan{}, "", "", err
	}
	plan.ID, plan.PlanID = physicalID, planID
	if (expectedPlanID != "" && strings.TrimSpace(planID) != strings.TrimSpace(expectedPlanID)) || plan.Owner != owner {
		return schedulersdk.ScheduledPlan{}, "", "", fmt.Errorf("Scheduler plan %q has inconsistent persisted identity", planID)
	}
	return plan, clientID, requestSHA256, nil
}
