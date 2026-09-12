package database

import (
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	schedulerpersistence "github.com/domainry/domainry-scheduler-sdk/persistence"
	_ "modernc.org/sqlite"
)

func TestScheduledPlanStorePersistsOwnerScopeAndIdempotencyAcrossReopen(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-plan-owner?mode=memory&cache=shared&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := EnsureSchema(t.Context(), db, "sqlite", ""); err != nil {
		t.Fatal(err)
	}
	dialect, err := Renderer("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewScheduledPlanStore(db, dialect, "runtime-a")
	if err != nil {
		t.Fatal(err)
	}
	owner := schedulersdk.ScheduledPlanOwner{WorkspaceID: "workspace-a", UserID: "user-a", ProductKey: "agent"}
	at := time.Date(2026, 9, 18, 1, 0, 0, 0, time.UTC)
	record := schedulerpersistence.ScheduledPlanRecord{
		ClientID: "client-once", RequestSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Plan: schedulersdk.ScheduledPlan{
			ID: "plan-once", Name: "周五提醒", Owner: owner, Timezone: "Asia/Shanghai", Trigger: schedulersdk.ScheduledPlanTrigger{Type: schedulersdk.ScheduledPlanTriggerOnce, At: &at},
			Input: json.RawMessage(`{"todo_status":"open"}`), AllowedActions: []string{"todo.list"}, Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "agent", Operation: "conversation_task_start"},
			ConversationRef: schedulersdk.ScheduledPlanConversationRef{ConversationID: "conversation-a", RunID: "run-a"}, Status: "enabled", Revision: 1, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		},
	}
	created, replay, err := store.CreateScheduledPlan(t.Context(), record)
	if err != nil || replay || created.ID != record.Plan.ID {
		t.Fatalf("created=%+v replay=%t err=%v", created, replay, err)
	}
	created, replay, err = store.CreateScheduledPlan(t.Context(), record)
	if err != nil || !replay || created.ConversationRef != record.Plan.ConversationRef {
		t.Fatalf("replayed=%+v replay=%t err=%v", created, replay, err)
	}
	conflict := record
	conflict.RequestSHA256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, _, err := store.CreateScheduledPlan(t.Context(), conflict); !errors.Is(err, schedulersdk.ErrScheduledPlanConflict) {
		t.Fatalf("changed replay err=%v", err)
	}
	reopened, err := NewScheduledPlanStore(db, dialect, "runtime-a")
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := reopened.GetScheduledPlan(t.Context(), schedulersdk.ScheduledPlanLookup{Owner: owner, PlanID: record.Plan.ID})
	if err != nil || loaded.Name != record.Plan.Name || string(loaded.Input) != string(record.Plan.Input) || len(loaded.AllowedActions) != 1 {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	recovery, err := reopened.ListScheduledPlansForRecovery(t.Context(), "", 1)
	if err != nil || len(recovery) != 1 || recovery[0].ID != record.Plan.ID {
		t.Fatalf("recovery=%+v err=%v", recovery, err)
	}
	recovery, err = reopened.ListScheduledPlansForRecovery(t.Context(), record.Plan.ID, 1)
	if err != nil || len(recovery) != 0 {
		t.Fatalf("recovery after cursor=%+v err=%v", recovery, err)
	}
	other := owner
	other.UserID = "user-b"
	if _, err := reopened.GetScheduledPlan(t.Context(), schedulersdk.ScheduledPlanLookup{Owner: other, PlanID: record.Plan.ID}); !errors.Is(err, schedulersdk.ErrScheduledPlanNotFound) {
		t.Fatalf("cross-user lookup err=%v", err)
	}
	var rows int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _scheduler_plans WHERE runtime_id = ? AND plan_id = ?`, "runtime-a", record.Plan.ID).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("rows=%d err=%v", rows, err)
	}
}

func TestScheduledPlanStoreConcurrentExactCreateWritesOneRecord(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-plan-concurrent?mode=memory&cache=shared&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := EnsureSchema(t.Context(), db, "sqlite", ""); err != nil {
		t.Fatal(err)
	}
	dialect, _ := Renderer("sqlite", "")
	store, _ := NewScheduledPlanStore(db, dialect, "runtime-a")
	record := schedulerpersistence.ScheduledPlanRecord{ClientID: "same", RequestSHA256: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", Plan: schedulersdk.ScheduledPlan{
		ID: "plan-same", Name: "same", Owner: schedulersdk.ScheduledPlanOwner{WorkspaceID: "workspace-a", UserID: "user-a", ProductKey: "agent"}, Timezone: "UTC",
		Trigger: schedulersdk.ScheduledPlanTrigger{Type: "recurring", Schedule: &schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60, Timezone: "UTC"}}, Input: json.RawMessage(`{}`),
		AllowedActions: []string{}, Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "agent", Operation: "conversation_task_start"}, Status: "enabled", Revision: 1, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}}
	const count = 16
	var wait sync.WaitGroup
	errorsOut := make(chan error, count)
	for range count {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, _, createErr := store.CreateScheduledPlan(t.Context(), record)
			errorsOut <- createErr
		}()
	}
	wait.Wait()
	close(errorsOut)
	for err := range errorsOut {
		if err != nil {
			t.Fatal(err)
		}
	}
	var rows int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _scheduler_plans`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("rows=%d err=%v", rows, err)
	}
}
