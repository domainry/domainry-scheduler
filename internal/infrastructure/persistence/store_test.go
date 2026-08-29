package persistence

import (
	"database/sql"
	"testing"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	_ "modernc.org/sqlite"
)

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
	first, err := NewStore(db, "sqlite", "", "runtime-a", "machine-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewStore(db, "sqlite", "", "runtime-a", "machine-b")
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
	if count != 1 {
		t.Fatalf("applied migrations=%d", count)
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
	store, err := NewStore(db, "sqlite", "", "runtime-a", "machine-a")
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
	if err := db.QueryRowContext(t.Context(), `SELECT status, last_error FROM scheduler_runs WHERE runtime_id = ? AND run_id = ?`, "runtime-a", run.Trigger.RunID).Scan(&status, &reason); err != nil {
		t.Fatal(err)
	}
	var deadLetters, events int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM scheduler_dead_letters WHERE runtime_id = ? AND run_id = ?`, "runtime-a", run.Trigger.RunID).Scan(&deadLetters); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM scheduler_run_events WHERE runtime_id = ? AND run_id = ? AND event_type = 'dead_lettered'`, "runtime-a", run.Trigger.RunID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if status != "dead_letter" || reason == "" || deadLetters != 1 || events != 1 {
		t.Fatalf("status=%q reason=%q dead_letters=%d events=%d", status, reason, deadLetters, events)
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
	store, err := NewStore(db, "sqlite", "", "runtime-a", "machine-a")
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
	if err := db.QueryRowContext(t.Context(), `SELECT next_run_at FROM scheduler_schedule_state WHERE runtime_id = ? AND definition_key = ?`, "runtime-a", "daily").Scan(&cursor); err != nil {
		t.Fatal(err)
	}
	if got := parseTime(cursor); !got.Equal(first) {
		t.Fatalf("same revision cursor=%s want=%s", got, first)
	}
	definition.Revision = "v2"
	changed := first.Add(2 * time.Hour)
	if err := store.Reconcile(t.Context(), definition, changed); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT next_run_at FROM scheduler_schedule_state WHERE runtime_id = ? AND definition_key = ?`, "runtime-a", "daily").Scan(&cursor); err != nil {
		t.Fatal(err)
	}
	if got := parseTime(cursor); !got.Equal(changed) {
		t.Fatalf("changed revision cursor=%s want=%s", got, changed)
	}
}
