package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	schedulerpersistence "github.com/domainry/domainry-scheduler-sdk/persistence"
)

type scheduledPlanRepositoryStub struct {
	record schedulerpersistence.ScheduledPlanRecord
}

func (s *scheduledPlanRepositoryStub) CreateScheduledPlan(_ context.Context, record schedulerpersistence.ScheduledPlanRecord) (schedulersdk.ScheduledPlan, bool, error) {
	if s.record.Plan.ID != "" {
		if s.record.RequestSHA256 != record.RequestSHA256 {
			return schedulersdk.ScheduledPlan{}, false, schedulersdk.ErrScheduledPlanConflict
		}
		return s.record.Plan, true, nil
	}
	s.record = record
	return record.Plan, false, nil
}
func (s *scheduledPlanRepositoryStub) GetScheduledPlan(_ context.Context, lookup schedulersdk.ScheduledPlanLookup) (schedulersdk.ScheduledPlan, error) {
	if s.record.Plan.ID != lookup.PlanID || s.record.Plan.Owner != lookup.Owner {
		return schedulersdk.ScheduledPlan{}, schedulersdk.ErrScheduledPlanNotFound
	}
	return s.record.Plan, nil
}
func (s *scheduledPlanRepositoryStub) ListScheduledPlans(_ context.Context, input schedulersdk.ScheduledPlanList) (schedulersdk.ScheduledPlanPage, error) {
	if s.record.Plan.ID == "" || s.record.Plan.Owner != input.Owner || s.record.Plan.Status == schedulersdk.ScheduledPlanStatusDeleted || input.Status != "" && input.Status != s.record.Plan.Status {
		return schedulersdk.ScheduledPlanPage{Items: []schedulersdk.ScheduledPlan{}}, nil
	}
	return schedulersdk.ScheduledPlanPage{Items: []schedulersdk.ScheduledPlan{s.record.Plan}}, nil
}
func (s *scheduledPlanRepositoryStub) ReplaceScheduledPlan(_ context.Context, plan schedulersdk.ScheduledPlan, expectedRevision int64) (schedulersdk.ScheduledPlan, error) {
	if s.record.Plan.ID != plan.ID || s.record.Plan.Owner != plan.Owner {
		return schedulersdk.ScheduledPlan{}, schedulersdk.ErrScheduledPlanNotFound
	}
	if s.record.Plan.Revision != expectedRevision {
		return schedulersdk.ScheduledPlan{}, schedulersdk.ErrScheduledPlanConflict
	}
	s.record.Plan = plan
	return plan, nil
}

func TestCreateScheduledPlanNormalizesAndReplaysExactOwnerCommand(t *testing.T) {
	repository := &scheduledPlanRepositoryStub{}
	runs := &hostStub{}
	service := NewService(t.Context(), nil, schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}, nil, nil, runs, nil, schedulersdk.DeploymentModeModule)
	service.SetScheduledPlanRepository(repository)
	service.now = func() time.Time { return time.Date(2026, 9, 12, 10, 0, 0, 123, time.UTC) }
	at := time.Date(2026, 9, 18, 9, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	input := schedulersdk.ScheduledPlanCreate{
		ClientID: " create-weekly ", Name: " 整理待办 ", Owner: schedulersdk.ScheduledPlanOwner{WorkspaceID: " workspace-a ", UserID: " user-a ", ProductKey: " agent "},
		Timezone: "Asia/Shanghai", Trigger: schedulersdk.ScheduledPlanTrigger{Type: schedulersdk.ScheduledPlanTriggerOnce, At: &at},
		Input: json.RawMessage(`{"week": 38, "goal": "review"}`), AllowedActions: []string{"todo.list", " artifact.create "},
		Target:          schedulersdk.TargetRef{Owner: "agent", Operation: " conversation_task_start "},
		ConversationRef: schedulersdk.ScheduledPlanConversationRef{ConversationID: " conversation-a ", RunID: " run-a "},
	}
	first, err := service.CreateScheduledPlan(t.Context(), input)
	if err != nil || first.Replay {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	second, err := service.CreateScheduledPlan(t.Context(), input)
	if err != nil || !second.Replay || second.Plan.ID != first.Plan.ID {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	plan := first.Plan
	if plan.Status != schedulersdk.ScheduledPlanStatusEnabled || plan.Revision != 1 || plan.Target.Type != "runtime_operation" || plan.Trigger.At == nil || plan.Trigger.At.Location() != time.UTC || plan.Trigger.Policy.Misfire != schedulersdk.ScheduledPlanMisfireCatchOne || plan.Trigger.Policy.MisfireGrace != time.Minute || plan.Trigger.Policy.MaxAttempts != 3 {
		t.Fatalf("normalized plan=%+v", plan)
	}
	if len(plan.AllowedActions) != 2 || plan.AllowedActions[0] != "artifact.create" || plan.AllowedActions[1] != "todo.list" || string(plan.Input) != `{"goal":"review","week":38}` {
		t.Fatalf("normalized actions=%v input=%s", plan.AllowedActions, plan.Input)
	}
	duplicateAfterNormalization := input
	duplicateAfterNormalization.ClientID = "duplicate-actions"
	duplicateAfterNormalization.AllowedActions = []string{"todo.list", " todo.list "}
	if _, err := service.CreateScheduledPlan(t.Context(), duplicateAfterNormalization); !errors.Is(err, schedulersdk.ErrScheduledPlanInvalid) {
		t.Fatalf("normalized duplicate actions err=%v", err)
	}
	loaded, err := service.GetScheduledPlan(t.Context(), schedulersdk.ScheduledPlanLookup{Owner: input.Owner, PlanID: plan.ID})
	if err != nil || loaded.ID != plan.ID {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	_, err = service.GetScheduledPlan(t.Context(), schedulersdk.ScheduledPlanLookup{Owner: schedulersdk.ScheduledPlanOwner{WorkspaceID: "workspace-a", UserID: "user-b", ProductKey: "agent"}, PlanID: plan.ID})
	if !errors.Is(err, schedulersdk.ErrScheduledPlanNotFound) {
		t.Fatalf("cross-user read err=%v", err)
	}
}

func TestScheduledPlanManagementUsesOwnerScopeRevisionCASAndDeletedTombstone(t *testing.T) {
	repository := &scheduledPlanRepositoryStub{}
	runs := &hostStub{}
	service := NewService(t.Context(), nil, schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}, nil, nil, runs, nil, schedulersdk.DeploymentModeModule)
	service.SetScheduledPlanRepository(repository)
	now := time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)
	service.now = func() time.Time { now = now.Add(time.Second); return now }
	owner := schedulersdk.ScheduledPlanOwner{WorkspaceID: "workspace-a", UserID: "user-a", ProductKey: "agent"}
	created, err := service.CreateScheduledPlan(t.Context(), schedulersdk.ScheduledPlanCreate{
		ClientID: "weekly", Name: "每周一整理待办", Owner: owner, Timezone: "Asia/Shanghai",
		Trigger: schedulersdk.ScheduledPlanTrigger{Type: schedulersdk.ScheduledPlanTriggerRecurring, Schedule: &schedulersdk.Schedule{Type: "weekly_at", TimeOfDay: "09:00", DayOfWeek: "monday", Timezone: "Asia/Shanghai"}},
		Input:   json.RawMessage(`{"goal":"整理本周待办","allowed_tools":["todo_list"]}`), AllowedActions: []string{"agent.conversation_tools.todo_list"},
		Target: schedulersdk.TargetRef{Owner: "agent", Operation: "conversation_task_start"}, ConversationRef: schedulersdk.ScheduledPlanConversationRef{ConversationID: "conversation-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err := service.ListScheduledPlans(t.Context(), schedulersdk.ScheduledPlanList{Owner: owner, Limit: 1})
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != created.Plan.ID {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	updated, err := service.UpdateScheduledPlan(t.Context(), schedulersdk.ScheduledPlanUpdate{
		Owner: owner, PlanID: created.Plan.ID, ExpectedRevision: 1, Name: "每周二整理待办", Timezone: "Asia/Shanghai",
		Trigger: schedulersdk.ScheduledPlanTrigger{Type: schedulersdk.ScheduledPlanTriggerRecurring, Schedule: &schedulersdk.Schedule{Type: "weekly_at", TimeOfDay: "10:30", DayOfWeek: "tuesday", Timezone: "Asia/Shanghai"}},
		Input:   created.Plan.Input, AllowedActions: created.Plan.AllowedActions, Target: created.Plan.Target, ConversationRef: created.Plan.ConversationRef,
	})
	if err != nil || updated.Plan.Revision != 2 || updated.Plan.Name != "每周二整理待办" || updated.Plan.Trigger.Schedule.DayOfWeek != "tuesday" {
		t.Fatalf("updated=%+v err=%v", updated, err)
	}
	if _, err := service.UpdateScheduledPlan(t.Context(), schedulersdk.ScheduledPlanUpdate{Owner: owner, PlanID: created.Plan.ID, ExpectedRevision: 1, Name: "stale", Timezone: updated.Plan.Timezone, Trigger: updated.Plan.Trigger, Input: updated.Plan.Input, AllowedActions: updated.Plan.AllowedActions, Target: updated.Plan.Target, ConversationRef: updated.Plan.ConversationRef}); !errors.Is(err, schedulersdk.ErrScheduledPlanConflict) {
		t.Fatalf("stale update err=%v", err)
	}
	paused, err := service.PauseScheduledPlan(t.Context(), schedulersdk.ScheduledPlanStatusChange{Owner: owner, PlanID: created.Plan.ID, ExpectedRevision: 2})
	if err != nil || paused.Plan.Status != schedulersdk.ScheduledPlanStatusPaused || paused.Plan.Revision != 3 {
		t.Fatalf("paused=%+v err=%v", paused, err)
	}
	if runs.planEnabled || runs.planDefinition.Status != schedulersdk.ScheduledPlanStatusPaused || runs.planDefinition.Revision != "3" {
		t.Fatalf("paused projection enabled=%v definition=%+v", runs.planEnabled, runs.planDefinition)
	}
	pauseReplay, err := service.PauseScheduledPlan(t.Context(), schedulersdk.ScheduledPlanStatusChange{Owner: owner, PlanID: created.Plan.ID, ExpectedRevision: 2})
	if err != nil || !pauseReplay.Replay || pauseReplay.Plan.Revision != 3 {
		t.Fatalf("pause replay=%+v err=%v", pauseReplay, err)
	}
	resumed, err := service.ResumeScheduledPlan(t.Context(), schedulersdk.ScheduledPlanStatusChange{Owner: owner, PlanID: created.Plan.ID, ExpectedRevision: 3})
	if err != nil || resumed.Plan.Status != schedulersdk.ScheduledPlanStatusEnabled || resumed.Plan.Revision != 4 {
		t.Fatalf("resumed=%+v err=%v", resumed, err)
	}
	if !runs.planEnabled || runs.planDefinition.Status != schedulersdk.ScheduledPlanStatusEnabled || runs.planDefinition.Revision != "4" || !runs.planNext.After(resumed.Plan.UpdatedAt) {
		t.Fatalf("resumed projection enabled=%v next=%s plan=%+v definition=%+v", runs.planEnabled, runs.planNext, resumed.Plan, runs.planDefinition)
	}
	deleted, err := service.DeleteScheduledPlan(t.Context(), schedulersdk.ScheduledPlanStatusChange{Owner: owner, PlanID: created.Plan.ID, ExpectedRevision: 4})
	if err != nil || !deleted.Deleted || deleted.Revision != 5 {
		t.Fatalf("deleted=%+v err=%v", deleted, err)
	}
	if runs.planEnabled || runs.planDefinition.Status != schedulersdk.ScheduledPlanStatusDeleted || runs.planDefinition.Revision != "5" {
		t.Fatalf("deleted projection enabled=%v definition=%+v", runs.planEnabled, runs.planDefinition)
	}
	deleteReplay, err := service.DeleteScheduledPlan(t.Context(), schedulersdk.ScheduledPlanStatusChange{Owner: owner, PlanID: created.Plan.ID, ExpectedRevision: 4})
	if err != nil || !deleteReplay.Replay || deleteReplay.Revision != 5 {
		t.Fatalf("delete replay=%+v err=%v", deleteReplay, err)
	}
	if _, err := service.GetScheduledPlan(t.Context(), schedulersdk.ScheduledPlanLookup{Owner: owner, PlanID: created.Plan.ID}); !errors.Is(err, schedulersdk.ErrScheduledPlanNotFound) {
		t.Fatalf("deleted get err=%v", err)
	}
	page, err = service.ListScheduledPlans(t.Context(), schedulersdk.ScheduledPlanList{Owner: owner})
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("deleted page=%+v err=%v", page, err)
	}
	if _, err := service.PauseScheduledPlan(t.Context(), schedulersdk.ScheduledPlanStatusChange{Owner: schedulersdk.ScheduledPlanOwner{WorkspaceID: "workspace-a", UserID: "other", ProductKey: "agent"}, PlanID: created.Plan.ID, ExpectedRevision: 4}); !errors.Is(err, schedulersdk.ErrScheduledPlanNotFound) {
		t.Fatalf("cross-owner pause err=%v", err)
	}
}

var _ schedulerpersistence.ScheduledPlanRepository = (*scheduledPlanRepositoryStub)(nil)

func (s *scheduledPlanRepositoryStub) ListScheduledPlansForRecovery(context.Context, string, int) ([]schedulersdk.ScheduledPlan, error) {
	if s.record.Plan.ID == "" {
		return nil, nil
	}
	return []schedulersdk.ScheduledPlan{s.record.Plan}, nil
}

var _ schedulerpersistence.ScheduledPlanRecoveryRepository = (*scheduledPlanRepositoryStub)(nil)
