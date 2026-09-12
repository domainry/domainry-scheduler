package scheduler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	schedulerpersistence "github.com/domainry/domainry-scheduler-sdk/persistence"
	"github.com/domainry/domainry-scheduler-sdk/schedule"
)

const scheduledPlanDefinitionPrefix = "scheduled-plan:"

type scheduledPlanRunProjector interface {
	ReconcileScheduledPlan(context.Context, schedulersdk.Definition, time.Time, bool) error
}

func (b *Service) SetScheduledPlanRepository(repository schedulerpersistence.ScheduledPlanRepository) {
	b.planRepository = repository
	if recovery, ok := repository.(schedulerpersistence.ScheduledPlanRecoveryRepository); ok {
		b.planRecovery = recovery
	}
}

func (b *Service) CreateScheduledPlan(ctx context.Context, input schedulersdk.ScheduledPlanCreate) (schedulersdk.ScheduledPlanReceipt, error) {
	if b.planRepository == nil {
		return schedulersdk.ScheduledPlanReceipt{}, fmt.Errorf("Scheduler plan repository is unavailable")
	}
	normalized, err := normalizeScheduledPlanCreate(input)
	if err != nil {
		return schedulersdk.ScheduledPlanReceipt{}, fmt.Errorf("%w: normalize input: %v", schedulersdk.ErrScheduledPlanInvalid, err)
	}
	if err := schedule.ValidateScheduledPlanCreate(ctx, normalized); err != nil {
		return schedulersdk.ScheduledPlanReceipt{}, err
	}
	request, err := json.Marshal(normalized)
	if err != nil {
		return schedulersdk.ScheduledPlanReceipt{}, err
	}
	digest := sha256.Sum256(request)
	now := b.now().UTC()
	plan := schedulersdk.ScheduledPlan{
		ID: scheduledPlanID(b.application.RuntimeID, normalized.Owner, normalized.ClientID), Name: normalized.Name,
		Owner: normalized.Owner, Timezone: normalized.Timezone, Trigger: normalized.Trigger, Input: normalized.Input,
		AllowedActions: normalized.AllowedActions, Target: normalized.Target, ConversationRef: normalized.ConversationRef,
		Status: normalized.Status, Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	created, replay, err := b.planRepository.CreateScheduledPlan(ctx, schedulerpersistence.ScheduledPlanRecord{
		Plan: plan, ClientID: normalized.ClientID, RequestSHA256: hex.EncodeToString(digest[:]),
	})
	if err != nil {
		return schedulersdk.ScheduledPlanReceipt{}, err
	}
	if err := b.projectScheduledPlan(ctx, created); err != nil {
		return schedulersdk.ScheduledPlanReceipt{}, err
	}
	return schedulersdk.ScheduledPlanReceipt{Plan: created, Replay: replay}, nil
}

func (b *Service) GetScheduledPlan(ctx context.Context, lookup schedulersdk.ScheduledPlanLookup) (schedulersdk.ScheduledPlan, error) {
	if b.planRepository == nil {
		return schedulersdk.ScheduledPlan{}, fmt.Errorf("Scheduler plan repository is unavailable")
	}
	if err := lookup.Owner.Validate(); err != nil {
		return schedulersdk.ScheduledPlan{}, err
	}
	lookup.PlanID = strings.TrimSpace(lookup.PlanID)
	if lookup.PlanID == "" {
		return schedulersdk.ScheduledPlan{}, fmt.Errorf("%w: plan ID is required", schedulersdk.ErrScheduledPlanInvalid)
	}
	lookup.Owner = normalizeScheduledPlanOwner(lookup.Owner)
	plan, err := b.planRepository.GetScheduledPlan(ctx, lookup)
	if err == nil && plan.Status == schedulersdk.ScheduledPlanStatusDeleted {
		err = schedulersdk.ErrScheduledPlanNotFound
	}
	return plan, err
}

func (b *Service) ListScheduledPlans(ctx context.Context, input schedulersdk.ScheduledPlanList) (schedulersdk.ScheduledPlanPage, error) {
	if b.planRepository == nil {
		return schedulersdk.ScheduledPlanPage{}, fmt.Errorf("Scheduler plan repository is unavailable")
	}
	input.Owner = normalizeScheduledPlanOwner(input.Owner)
	input.Status, input.Cursor = strings.TrimSpace(input.Status), strings.TrimSpace(input.Cursor)
	if err := input.Owner.Validate(); err != nil {
		return schedulersdk.ScheduledPlanPage{}, err
	}
	if input.Limit < 0 || input.Limit > 100 {
		return schedulersdk.ScheduledPlanPage{}, fmt.Errorf("%w: list limit must be between 1 and 100 when set", schedulersdk.ErrScheduledPlanInvalid)
	}
	if input.Cursor != "" && !scheduledPlanManagementKey(input.Cursor) {
		return schedulersdk.ScheduledPlanPage{}, fmt.Errorf("%w: list cursor is invalid", schedulersdk.ErrScheduledPlanInvalid)
	}
	switch input.Status {
	case "", schedulersdk.ScheduledPlanStatusEnabled, schedulersdk.ScheduledPlanStatusDisabled, schedulersdk.ScheduledPlanStatusPaused:
	default:
		return schedulersdk.ScheduledPlanPage{}, fmt.Errorf("%w: list status is invalid", schedulersdk.ErrScheduledPlanInvalid)
	}
	if input.Limit == 0 {
		input.Limit = 50
	}
	return b.planRepository.ListScheduledPlans(ctx, input)
}

func (b *Service) UpdateScheduledPlan(ctx context.Context, input schedulersdk.ScheduledPlanUpdate) (schedulersdk.ScheduledPlanReceipt, error) {
	current, err := b.scheduledPlanForMutation(ctx, input.Owner, input.PlanID, input.ExpectedRevision)
	if err != nil {
		return schedulersdk.ScheduledPlanReceipt{}, err
	}
	normalized, err := normalizeScheduledPlanCreate(schedulersdk.ScheduledPlanCreate{
		ClientID: "management-update", Name: input.Name, Owner: input.Owner, Timezone: input.Timezone,
		Trigger: input.Trigger, Input: input.Input, AllowedActions: input.AllowedActions, Target: input.Target,
		ConversationRef: input.ConversationRef, Status: current.Status,
	})
	if err != nil {
		return schedulersdk.ScheduledPlanReceipt{}, fmt.Errorf("%w: normalize update: %v", schedulersdk.ErrScheduledPlanInvalid, err)
	}
	if err := schedule.ValidateScheduledPlanCreate(ctx, normalized); err != nil {
		return schedulersdk.ScheduledPlanReceipt{}, err
	}
	desired := schedulersdk.ScheduledPlan{
		ID: current.ID, Name: normalized.Name, Owner: current.Owner, Timezone: normalized.Timezone,
		Trigger: normalized.Trigger, Input: normalized.Input, AllowedActions: normalized.AllowedActions,
		Target: normalized.Target, ConversationRef: normalized.ConversationRef, Status: current.Status,
		Revision: input.ExpectedRevision + 1, CreatedAt: current.CreatedAt,
	}
	if current.Revision == desired.Revision && scheduledPlanMutableEqual(current, desired) {
		if err := b.projectScheduledPlan(ctx, current); err != nil {
			return schedulersdk.ScheduledPlanReceipt{}, err
		}
		return schedulersdk.ScheduledPlanReceipt{Plan: current, Replay: true}, nil
	}
	if current.Revision != input.ExpectedRevision {
		return schedulersdk.ScheduledPlanReceipt{}, schedulersdk.ErrScheduledPlanConflict
	}
	desired.UpdatedAt = b.now().UTC()
	stored, err := b.planRepository.ReplaceScheduledPlan(ctx, desired, input.ExpectedRevision)
	if err != nil {
		return schedulersdk.ScheduledPlanReceipt{}, err
	}
	if err := b.projectScheduledPlan(ctx, stored); err != nil {
		return schedulersdk.ScheduledPlanReceipt{}, err
	}
	return schedulersdk.ScheduledPlanReceipt{Plan: stored}, nil
}

func (b *Service) PauseScheduledPlan(ctx context.Context, input schedulersdk.ScheduledPlanStatusChange) (schedulersdk.ScheduledPlanReceipt, error) {
	return b.changeScheduledPlanStatus(ctx, input, schedulersdk.ScheduledPlanStatusPaused)
}

func (b *Service) ResumeScheduledPlan(ctx context.Context, input schedulersdk.ScheduledPlanStatusChange) (schedulersdk.ScheduledPlanReceipt, error) {
	return b.changeScheduledPlanStatus(ctx, input, schedulersdk.ScheduledPlanStatusEnabled)
}

func (b *Service) DeleteScheduledPlan(ctx context.Context, input schedulersdk.ScheduledPlanStatusChange) (schedulersdk.ScheduledPlanDeleteReceipt, error) {
	if b.planRepository == nil {
		return schedulersdk.ScheduledPlanDeleteReceipt{}, fmt.Errorf("Scheduler plan repository is unavailable")
	}
	owner, planID, err := normalizeScheduledPlanMutation(input.Owner, input.PlanID, input.ExpectedRevision)
	if err != nil {
		return schedulersdk.ScheduledPlanDeleteReceipt{}, err
	}
	current, err := b.planRepository.GetScheduledPlan(ctx, schedulersdk.ScheduledPlanLookup{Owner: owner, PlanID: planID})
	if err != nil {
		return schedulersdk.ScheduledPlanDeleteReceipt{}, err
	}
	if current.Status == schedulersdk.ScheduledPlanStatusDeleted {
		if current.Revision == input.ExpectedRevision+1 {
			if err := b.projectScheduledPlan(ctx, current); err != nil {
				return schedulersdk.ScheduledPlanDeleteReceipt{}, err
			}
			return schedulersdk.ScheduledPlanDeleteReceipt{PlanID: current.ID, Revision: current.Revision, Deleted: true, Replay: true}, nil
		}
		return schedulersdk.ScheduledPlanDeleteReceipt{}, schedulersdk.ErrScheduledPlanNotFound
	}
	if current.Revision != input.ExpectedRevision {
		return schedulersdk.ScheduledPlanDeleteReceipt{}, schedulersdk.ErrScheduledPlanConflict
	}
	deleted := current
	deleted.Status, deleted.Revision, deleted.UpdatedAt = schedulersdk.ScheduledPlanStatusDeleted, current.Revision+1, b.now().UTC()
	stored, err := b.planRepository.ReplaceScheduledPlan(ctx, deleted, current.Revision)
	if err != nil {
		return schedulersdk.ScheduledPlanDeleteReceipt{}, err
	}
	if err := b.projectScheduledPlan(ctx, stored); err != nil {
		return schedulersdk.ScheduledPlanDeleteReceipt{}, err
	}
	return schedulersdk.ScheduledPlanDeleteReceipt{PlanID: stored.ID, Revision: stored.Revision, Deleted: true}, nil
}

func (b *Service) changeScheduledPlanStatus(ctx context.Context, input schedulersdk.ScheduledPlanStatusChange, desiredStatus string) (schedulersdk.ScheduledPlanReceipt, error) {
	current, err := b.scheduledPlanForMutation(ctx, input.Owner, input.PlanID, input.ExpectedRevision)
	if err != nil {
		return schedulersdk.ScheduledPlanReceipt{}, err
	}
	if current.Revision == input.ExpectedRevision+1 && current.Status == desiredStatus {
		if err := b.projectScheduledPlan(ctx, current); err != nil {
			return schedulersdk.ScheduledPlanReceipt{}, err
		}
		return schedulersdk.ScheduledPlanReceipt{Plan: current, Replay: true}, nil
	}
	if current.Revision != input.ExpectedRevision {
		return schedulersdk.ScheduledPlanReceipt{}, schedulersdk.ErrScheduledPlanConflict
	}
	if current.Status == desiredStatus {
		return schedulersdk.ScheduledPlanReceipt{Plan: current, Replay: true}, nil
	}
	if desiredStatus == schedulersdk.ScheduledPlanStatusPaused && current.Status != schedulersdk.ScheduledPlanStatusEnabled {
		return schedulersdk.ScheduledPlanReceipt{}, schedulersdk.ErrScheduledPlanConflict
	}
	if desiredStatus == schedulersdk.ScheduledPlanStatusEnabled && current.Status != schedulersdk.ScheduledPlanStatusPaused && current.Status != schedulersdk.ScheduledPlanStatusDisabled {
		return schedulersdk.ScheduledPlanReceipt{}, schedulersdk.ErrScheduledPlanConflict
	}
	updated := current
	updated.Status, updated.Revision, updated.UpdatedAt = desiredStatus, current.Revision+1, b.now().UTC()
	stored, err := b.planRepository.ReplaceScheduledPlan(ctx, updated, current.Revision)
	if err != nil {
		return schedulersdk.ScheduledPlanReceipt{}, err
	}
	if err := b.projectScheduledPlan(ctx, stored); err != nil {
		return schedulersdk.ScheduledPlanReceipt{}, err
	}
	return schedulersdk.ScheduledPlanReceipt{Plan: stored}, nil
}

func (b *Service) scheduledPlanForMutation(ctx context.Context, owner schedulersdk.ScheduledPlanOwner, planID string, revision int64) (schedulersdk.ScheduledPlan, error) {
	if b.planRepository == nil {
		return schedulersdk.ScheduledPlan{}, fmt.Errorf("Scheduler plan repository is unavailable")
	}
	owner, planID, err := normalizeScheduledPlanMutation(owner, planID, revision)
	if err != nil {
		return schedulersdk.ScheduledPlan{}, err
	}
	plan, err := b.planRepository.GetScheduledPlan(ctx, schedulersdk.ScheduledPlanLookup{Owner: owner, PlanID: planID})
	if err == nil && plan.Status == schedulersdk.ScheduledPlanStatusDeleted {
		err = schedulersdk.ErrScheduledPlanNotFound
	}
	return plan, err
}

func normalizeScheduledPlanMutation(owner schedulersdk.ScheduledPlanOwner, planID string, revision int64) (schedulersdk.ScheduledPlanOwner, string, error) {
	owner, planID = normalizeScheduledPlanOwner(owner), strings.TrimSpace(planID)
	if err := owner.Validate(); err != nil {
		return owner, planID, err
	}
	if !scheduledPlanManagementKey(planID) || revision < 1 {
		return owner, planID, fmt.Errorf("%w: plan ID and positive expected revision are required", schedulersdk.ErrScheduledPlanInvalid)
	}
	return owner, planID, nil
}

func scheduledPlanManagementKey(value string) bool {
	if value == "" || len(value) > 191 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character == 0x7f {
			return false
		}
	}
	return true
}

func scheduledPlanMutableEqual(left, right schedulersdk.ScheduledPlan) bool {
	return left.Name == right.Name && left.Timezone == right.Timezone && reflect.DeepEqual(left.Trigger, right.Trigger) &&
		bytes.Equal(left.Input, right.Input) && reflect.DeepEqual(left.AllowedActions, right.AllowedActions) &&
		reflect.DeepEqual(left.Target, right.Target) && reflect.DeepEqual(left.ConversationRef, right.ConversationRef) && left.Status == right.Status
}

func normalizeScheduledPlanCreate(value schedulersdk.ScheduledPlanCreate) (schedulersdk.ScheduledPlanCreate, error) {
	value.ClientID = strings.TrimSpace(value.ClientID)
	value.Name = strings.TrimSpace(value.Name)
	value.Owner = normalizeScheduledPlanOwner(value.Owner)
	value.Timezone = strings.TrimSpace(value.Timezone)
	value.Trigger.Type = strings.TrimSpace(value.Trigger.Type)
	if value.Trigger.At != nil {
		at := value.Trigger.At.UTC()
		value.Trigger.At = &at
	}
	if value.Trigger.Schedule != nil {
		rule := *value.Trigger.Schedule
		if strings.TrimSpace(rule.Timezone) == "" {
			rule.Timezone = value.Timezone
		}
		rule.Type, rule.Expression, rule.Timezone = strings.TrimSpace(rule.Type), strings.TrimSpace(rule.Expression), strings.TrimSpace(rule.Timezone)
		rule.TimeOfDay, rule.DayOfWeek = strings.TrimSpace(rule.TimeOfDay), strings.TrimSpace(rule.DayOfWeek)
		value.Trigger.Schedule = &rule
	}
	value.Target = value.Target.Normalize()
	value.Target.Owner = strings.TrimSpace(value.Target.Owner)
	value.Target.Operation = strings.TrimSpace(value.Target.Operation)
	value.Target.ConnectionKey = strings.TrimSpace(value.Target.ConnectionKey)
	value.Target.DispatchMode = strings.TrimSpace(value.Target.DispatchMode)
	value.ConversationRef.ConversationID = strings.TrimSpace(value.ConversationRef.ConversationID)
	value.ConversationRef.RunID = strings.TrimSpace(value.ConversationRef.RunID)
	if strings.TrimSpace(value.Status) == "" {
		value.Status = schedulersdk.ScheduledPlanStatusEnabled
	} else {
		value.Status = strings.TrimSpace(value.Status)
	}
	value.Trigger.Policy = normalizeScheduledPlanPolicy(value.Trigger.Type, value.Trigger.Policy)
	actions := make([]string, len(value.AllowedActions))
	for index, action := range value.AllowedActions {
		actions[index] = strings.TrimSpace(action)
	}
	sort.Strings(actions)
	value.AllowedActions = actions
	decoder := json.NewDecoder(bytes.NewReader(value.Input))
	decoder.UseNumber()
	var input map[string]any
	if err := decoder.Decode(&input); err != nil {
		return schedulersdk.ScheduledPlanCreate{}, err
	}
	value.Input, _ = json.Marshal(input)
	return value, nil
}

func (b *Service) hydrateScheduledPlans(ctx context.Context) (bool, error) {
	if b.planRecovery == nil {
		return false, nil
	}
	found := false
	after := ""
	for {
		plans, err := b.planRecovery.ListScheduledPlansForRecovery(ctx, after, 200)
		if err != nil {
			return false, fmt.Errorf("load scheduled plans for recovery: %w", err)
		}
		for _, plan := range plans {
			if err := b.projectScheduledPlan(ctx, plan); err != nil {
				return false, err
			}
			found = true
		}
		if len(plans) < 200 {
			return found, nil
		}
		next := plans[len(plans)-1].ID
		if next <= after {
			return false, fmt.Errorf("scheduled plan recovery cursor did not advance")
		}
		after = next
	}
}

func (b *Service) projectScheduledPlan(ctx context.Context, plan schedulersdk.ScheduledPlan) error {
	projector, ok := b.runs.(scheduledPlanRunProjector)
	if !ok {
		return fmt.Errorf("Scheduler run store cannot project scheduled plans")
	}
	dispatch := schedulersdk.ScheduledPlanDispatch{
		ContractVersion: schedulersdk.ScheduledPlanDispatchContractVersion,
		PlanID:          plan.ID, Owner: plan.Owner, Input: append(json.RawMessage(nil), plan.Input...),
		AllowedActions: append([]string(nil), plan.AllowedActions...), ConversationRef: plan.ConversationRef,
	}
	payload, err := json.Marshal(dispatch)
	if err != nil {
		return err
	}
	target := plan.Target.Normalize()
	target.Payload = payload
	definition := schedulersdk.Definition{
		Key: scheduledPlanDefinitionPrefix + plan.ID, Name: plan.Name, Status: plan.Status,
		Revision: strconv.FormatInt(plan.Revision, 10), Target: target, Policy: normalizeScheduledPlanPolicy(plan.Trigger.Type, plan.Trigger.Policy),
	}
	var next time.Time
	switch plan.Trigger.Type {
	case schedulersdk.ScheduledPlanTriggerOnce:
		if plan.Trigger.At == nil {
			return fmt.Errorf("project scheduled plan %s: missing one-time instant", plan.ID)
		}
		next = plan.Trigger.At.UTC()
		definition.Schedule = schedulersdk.Schedule{Type: "once", Expression: next.Format(time.RFC3339Nano), Timezone: plan.Timezone}
	case schedulersdk.ScheduledPlanTriggerRecurring:
		if plan.Trigger.Schedule == nil {
			return fmt.Errorf("project scheduled plan %s: missing recurring schedule", plan.ID)
		}
		definition.Schedule = *plan.Trigger.Schedule
		anchor := plan.CreatedAt.UTC()
		if plan.Revision > 1 && !plan.UpdatedAt.IsZero() {
			anchor = plan.UpdatedAt.UTC()
		}
		next = schedule.NextSchedule(definition.Schedule, anchor)
	default:
		return fmt.Errorf("project scheduled plan %s: invalid trigger type", plan.ID)
	}
	enabled := plan.Status == schedulersdk.ScheduledPlanStatusEnabled
	if err := projector.ReconcileScheduledPlan(ctx, definition, next, enabled); err != nil {
		return fmt.Errorf("project scheduled plan %s: %w", plan.ID, err)
	}
	return nil
}

func normalizeScheduledPlanPolicy(triggerType string, policy schedulersdk.Policy) schedulersdk.Policy {
	policy.Misfire = strings.TrimSpace(policy.Misfire)
	if policy.Misfire == "" {
		if triggerType == schedulersdk.ScheduledPlanTriggerOnce {
			policy.Misfire = schedulersdk.ScheduledPlanMisfireCatchOne
		} else {
			policy.Misfire = schedulersdk.ScheduledPlanMisfireSkip
		}
	}
	if policy.MisfireGrace == 0 {
		policy.MisfireGrace = time.Minute
	}
	if policy.MaxAttempts == 0 {
		policy.MaxAttempts = 3
	}
	if policy.RetryInitial == 0 {
		policy.RetryInitial = 30 * time.Second
	}
	if policy.RetryMax == 0 {
		policy.RetryMax = 15 * time.Minute
	}
	if policy.Timeout == 0 {
		policy.Timeout = 5 * time.Minute
	}
	return policy
}

func normalizeScheduledPlanOwner(value schedulersdk.ScheduledPlanOwner) schedulersdk.ScheduledPlanOwner {
	value.WorkspaceID = strings.TrimSpace(value.WorkspaceID)
	value.UserID = strings.TrimSpace(value.UserID)
	value.ProductKey = strings.TrimSpace(value.ProductKey)
	return value
}

func scheduledPlanID(runtimeID string, owner schedulersdk.ScheduledPlanOwner, clientID string) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{
		strings.TrimSpace(runtimeID), owner.WorkspaceID, owner.UserID, owner.ProductKey, strings.TrimSpace(clientID),
	}, "\x00")))
	return "plan_" + hex.EncodeToString(digest[:16])
}

var _ schedulersdk.ScheduledPlanService = (*Service)(nil)
