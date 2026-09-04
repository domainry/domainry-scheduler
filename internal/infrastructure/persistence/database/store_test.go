package database

import (
	"database/sql"
	"testing"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulermodel "github.com/domainry/domainry-scheduler/internal/domain/scheduler/model"
	_ "modernc.org/sqlite"
)

func TestCommandReceiptClaimIsDurableAndRuntimeScoped(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-command-receipt?mode=memory&cache=shared")
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
	first, err := NewCommandReceiptStore(db, dialect, "runtime-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewCommandReceiptStore(db, dialect, "runtime-a")
	if err != nil {
		t.Fatal(err)
	}
	claim := schedulermodel.CommandReceipt{IdempotencyKey: "run-01", ActionKey: "scheduler.definitions.run", ResourceKey: "/scheduler/definitions/daily/run", RequestHash: "hash-a"}
	receipt, claimed, err := first.ClaimCommand(t.Context(), claim)
	if err != nil || !claimed || receipt.Status != schedulermodel.CommandReceiptExecuting {
		t.Fatalf("first claim=%+v claimed=%t err=%v", receipt, claimed, err)
	}
	receipt, claimed, err = second.ClaimCommand(t.Context(), claim)
	if err != nil || claimed || receipt.Status != schedulermodel.CommandReceiptExecuting {
		t.Fatalf("concurrent claim=%+v claimed=%t err=%v", receipt, claimed, err)
	}
	response := []byte(`{"id":"run-1","status":"succeeded"}`)
	if err := first.CompleteCommand(t.Context(), claim.IdempotencyKey, claim.RequestHash, 200, response); err != nil {
		t.Fatal(err)
	}
	receipt, claimed, err = second.ClaimCommand(t.Context(), claim)
	if err != nil || claimed || receipt.Status != schedulermodel.CommandReceiptCompleted || receipt.HTTPStatus != 200 || string(receipt.ResponseJSON) != string(response) {
		t.Fatalf("replay=%+v claimed=%t err=%v", receipt, claimed, err)
	}
	otherRuntime, err := NewCommandReceiptStore(db, dialect, "runtime-b")
	if err != nil {
		t.Fatal(err)
	}
	if _, claimed, err := otherRuntime.ClaimCommand(t.Context(), claim); err != nil || !claimed {
		t.Fatalf("other runtime claimed=%t err=%v", claimed, err)
	}
}

func TestDatabaseClaimRunsOnOnlyOneMachine(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-claim?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	migrations, err := SchemaMigrations("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		for _, statement := range migration.Statements {
			if _, err := db.ExecContext(t.Context(), statement); err != nil {
				t.Fatal(err)
			}
		}
	}
	dialect, err := Renderer("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewStore(db, dialect, "runtime-a", "machine-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewStore(db, dialect, "runtime-a", "machine-b")
	if err != nil {
		t.Fatal(err)
	}
	definition := schedulersdk.Definition{Key: "daily", Revision: "v1", Status: "enabled", Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60}, Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "workflow", Operation: "run"}}
	dueAt := time.Date(2026, 8, 30, 1, 0, 0, 0, time.UTC)
	if err := first.Reconcile(t.Context(), definition, dueAt); err != nil {
		t.Fatal(err)
	}
	due := modulehost.DueTrigger{Definition: definition, ScheduledFor: dueAt}
	runA, claimedA, err := first.Claim(t.Context(), due, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, claimedB, err := second.Claim(t.Context(), due, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !claimedA || claimedB || runA.Lease.Owner != "machine-a" || runA.Lease.Token != 1 {
		t.Fatalf("first=%t second=%t lease=%+v", claimedA, claimedB, runA.Lease)
	}
	if err := first.Accept(t.Context(), runA, schedulersdk.DownstreamReceipt{ID: "receipt-1", Owner: "workflow", Status: "accepted"}); err != nil {
		t.Fatal(err)
	}
	runs, err := second.List(t.Context(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].Status != "succeeded" || runs[0].DownstreamReceipt.ID != "receipt-1" {
		t.Fatalf("runs=%+v", runs)
	}
}

func TestStandaloneSchemaMigrationIsIdempotent(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-migration?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := EnsureSchema(t.Context(), db, "sqlite", ""); err != nil {
		t.Fatal(err)
	}
	if err := EnsureSchema(t.Context(), db, "sqlite", ""); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM "_schema_migrations" WHERE "dirty" = FALSE`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("applied migrations=%d", count)
	}
	var definitions int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '_scheduler_definitions'`).Scan(&definitions); err != nil {
		t.Fatal(err)
	}
	if definitions != 1 {
		t.Fatalf("scheduler definition tables=%d", definitions)
	}
}

func TestFailureAtMaximumAttemptAtomicallyCreatesDeadLetter(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-dead-letter?mode=memory&cache=shared")
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
	store, err := NewStore(db, dialect, "runtime-a", "machine-a")
	if err != nil {
		t.Fatal(err)
	}
	dueAt := time.Date(2026, 8, 30, 2, 0, 0, 0, time.UTC)
	definition := schedulersdk.Definition{Key: "once", Revision: "v1", Status: "enabled", Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60}, Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "workflow", Operation: "run"}, Policy: schedulersdk.Policy{MaxAttempts: 1}}
	if err := store.Reconcile(t.Context(), definition, dueAt); err != nil {
		t.Fatal(err)
	}
	run, claimed, err := store.Claim(t.Context(), modulehost.DueTrigger{Definition: definition, ScheduledFor: dueAt}, time.Minute)
	if err != nil || !claimed {
		t.Fatalf("claimed=%t err=%v", claimed, err)
	}
	if err := store.Fail(t.Context(), run, sql.ErrConnDone, dueAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	var status, reason string
	if err := db.QueryRowContext(t.Context(), `SELECT status, last_error FROM _scheduler_runs WHERE runtime_id = ? AND run_id = ?`, "runtime-a", run.Trigger.RunID).Scan(&status, &reason); err != nil {
		t.Fatal(err)
	}
	var deadLetters, events int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _scheduler_dead_letters WHERE runtime_id = ? AND run_id = ?`, "runtime-a", run.Trigger.RunID).Scan(&deadLetters); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _scheduler_run_events WHERE runtime_id = ? AND run_id = ? AND event_type = 'dead_lettered'`, "runtime-a", run.Trigger.RunID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if status != "dead_letter" || reason == "" || deadLetters != 1 || events != 1 {
		t.Fatalf("status=%q reason=%q dead_letters=%d events=%d", status, reason, deadLetters, events)
	}
	listed, err := store.DeadLetters(t.Context(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].RunID != run.Trigger.RunID || listed[0].DefinitionKey != definition.Key || listed[0].Status != "open" || listed[0].Reason == "" {
		t.Fatalf("listed dead letters=%+v", listed)
	}
}

func TestReconcilePreservesCursorUntilDefinitionRevisionChanges(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-reconcile-cursor?mode=memory&cache=shared")
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
	store, err := NewStore(db, dialect, "runtime-a", "machine-a")
	if err != nil {
		t.Fatal(err)
	}
	definition := schedulersdk.Definition{Key: "daily", Revision: "v1", Status: "enabled", Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60}, Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "workflow", Operation: "run"}}
	first := time.Date(2026, 8, 30, 3, 0, 0, 0, time.UTC)
	if err := store.Reconcile(t.Context(), definition, first); err != nil {
		t.Fatal(err)
	}
	if err := store.Reconcile(t.Context(), definition, first.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	var cursor string
	if err := db.QueryRowContext(t.Context(), `SELECT next_run_at FROM _scheduler_definition_states WHERE runtime_id = ? AND definition_key = ?`, "runtime-a", "daily").Scan(&cursor); err != nil {
		t.Fatal(err)
	}
	if got, _ := time.Parse(time.RFC3339Nano, cursor); !got.Equal(first) {
		t.Fatalf("same revision cursor=%s want=%s", got, first)
	}
	manual := first.Add(90 * time.Minute)
	if err := store.Reschedule(t.Context(), definition.Key, manual, "operator correction"); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT next_run_at FROM _scheduler_definition_states WHERE runtime_id = ? AND definition_key = ?`, "runtime-a", "daily").Scan(&cursor); err != nil {
		t.Fatal(err)
	}
	if got, _ := time.Parse(time.RFC3339Nano, cursor); !got.Equal(manual) {
		t.Fatalf("manual reschedule cursor=%s want=%s", got, manual)
	}
	definition.Revision = "v2"
	changed := first.Add(2 * time.Hour)
	if err := store.Reconcile(t.Context(), definition, changed); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT next_run_at FROM _scheduler_definition_states WHERE runtime_id = ? AND definition_key = ?`, "runtime-a", "daily").Scan(&cursor); err != nil {
		t.Fatal(err)
	}
	if got, _ := time.Parse(time.RFC3339Nano, cursor); !got.Equal(changed) {
		t.Fatalf("changed revision cursor=%s want=%s", got, changed)
	}
}
