package database

import (
	"database/sql"
	"sync"
	"testing"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	_ "modernc.org/sqlite"
)

func TestScheduledPlanProjectionKeepsSourceBoundaryAndAppliesMisfirePolicies(t *testing.T) {
	db := openScheduledPlanExecutionDatabase(t, "misfire")
	defer db.Close()
	dialect, _ := Renderer("sqlite", "")
	store, _ := NewStore(db, dialect, "runtime-plan", "worker-a")
	base := time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC)

	runtimeDefinition := executionDefinition("runtime-daily", schedulersdk.Policy{})
	if err := store.Reconcile(t.Context(), runtimeDefinition, base.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	skip := executionDefinition("scheduled-plan:skip", schedulersdk.Policy{Misfire: schedulersdk.ScheduledPlanMisfireSkip, MisfireGrace: 30 * time.Second, MaxAttempts: 3})
	if err := store.ReconcileScheduledPlan(t.Context(), skip, base, true); err != nil {
		t.Fatal(err)
	}
	if err := store.DisableMissing(t.Context(), []string{runtimeDefinition.Key}, 2); err != nil {
		t.Fatal(err)
	}
	due, err := store.Due(t.Context(), base.Add(5*time.Minute+30*time.Second), 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("skip due=%+v err=%v", due, err)
	}
	assertDefinitionCursor(t, db, skip.Key, true, base.Add(6*time.Minute), "misfire_skipped", "scheduled_plan")

	catchOne := executionDefinition("scheduled-plan:catch-one", schedulersdk.Policy{Misfire: schedulersdk.ScheduledPlanMisfireCatchOne, MisfireGrace: 30 * time.Second, MaxAttempts: 3})
	if err := store.ReconcileScheduledPlan(t.Context(), catchOne, base, true); err != nil {
		t.Fatal(err)
	}
	due, err = store.Due(t.Context(), base.Add(5*time.Minute+30*time.Second), 10)
	if err != nil || len(due) != 1 || !due[0].ScheduledFor.Equal(base) || !due[0].ExpectedCursor.Equal(base) || !due[0].NextRunAt.Equal(base.Add(6*time.Minute)) {
		t.Fatalf("catch-one due=%+v err=%v", due, err)
	}
	run, claimed, err := store.Claim(t.Context(), due[0], time.Minute)
	if err != nil || !claimed {
		t.Fatalf("catch-one run=%+v claimed=%t err=%v", run, claimed, err)
	}
	if err := store.Accept(t.Context(), run, schedulersdk.DownstreamReceipt{ID: "catch-one-receipt", Owner: "agent", Status: "accepted"}); err != nil {
		t.Fatal(err)
	}
	assertDefinitionCursor(t, db, catchOne.Key, true, base.Add(6*time.Minute), "leased", "scheduled_plan")

	bounded := executionDefinition("scheduled-plan:bounded", schedulersdk.Policy{Misfire: schedulersdk.ScheduledPlanMisfireCatchMany, MisfireGrace: 30 * time.Second, MaxCatchupWindows: 3, MaxAttempts: 3})
	if err := store.ReconcileScheduledPlan(t.Context(), bounded, base, true); err != nil {
		t.Fatal(err)
	}
	due, err = store.Due(t.Context(), base.Add(5*time.Minute+30*time.Second), 10)
	if err != nil || len(due) != 3 || !due[2].NextRunAt.Equal(base.Add(6*time.Minute)) {
		t.Fatalf("bounded due=%+v err=%v", due, err)
	}
	for index, item := range due {
		run, claimed, claimErr := store.Claim(t.Context(), item, time.Minute)
		if claimErr != nil || !claimed {
			t.Fatalf("bounded index=%d run=%+v claimed=%t err=%v", index, run, claimed, claimErr)
		}
		if err := store.Accept(t.Context(), run, schedulersdk.DownstreamReceipt{ID: run.Trigger.RunID, Owner: "agent", Status: "accepted"}); err != nil {
			t.Fatal(err)
		}
	}
	assertDefinitionCursor(t, db, bounded.Key, true, base.Add(6*time.Minute), "leased", "scheduled_plan")
}

func TestRuntimeDefinitionSkipsClosedBusinessCalendarDatesWithoutCreatingRuns(t *testing.T) {
	db := openScheduledPlanExecutionDatabase(t, "business-calendar-skip")
	defer db.Close()
	dialect, _ := Renderer("sqlite", "")
	store, _ := NewStore(db, dialect, "runtime-plan", "worker-a")
	calendar := weekdayCalendarProtocol()
	definition := schedulersdk.Definition{
		Key: "weekday-daily", Name: "Weekday daily", Status: "enabled", Revision: "1|business_calendar:weekday@1",
		Schedule: schedulersdk.Schedule{Type: "daily_at", TimeOfDay: "09:00", Timezone: "UTC", BusinessCalendar: calendar, NonWorkingDayPolicy: schedulersdk.NonWorkingDaySkip},
		Target:   schedulersdk.TargetRef{Type: "runtime_operation", Owner: "workflow", Operation: "scheduled:daily"},
		Policy:   schedulersdk.Policy{Misfire: schedulersdk.ScheduledPlanMisfireCatchMany, MaxCatchupWindows: 5},
	}
	saturday := time.Date(2026, 9, 19, 9, 0, 0, 0, time.UTC)
	if err := store.Reconcile(t.Context(), definition, saturday); err != nil {
		t.Fatal(err)
	}
	due, err := store.Due(t.Context(), time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC), 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("closed-date due=%+v err=%v", due, err)
	}
	assertDefinitionCursor(t, db, definition.Key, true, time.Date(2026, 9, 21, 9, 0, 0, 0, time.UTC), "misfire_skipped", "runtime_definition")
	var runs int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _scheduler_runs WHERE runtime_id = ? AND definition_key = ?`, "runtime-plan", definition.Key).Scan(&runs); err != nil || runs != 0 {
		t.Fatalf("runs=%d err=%v", runs, err)
	}
}

func TestScheduledPlanWindowHasOneConcurrentClaim(t *testing.T) {
	db := openScheduledPlanExecutionDatabase(t, "concurrent-claim")
	defer db.Close()
	dialect, _ := Renderer("sqlite", "")
	first, _ := NewStore(db, dialect, "runtime-plan", "worker-a")
	second, _ := NewStore(db, dialect, "runtime-plan", "worker-b")
	dueAt := time.Now().UTC().Add(-time.Second)
	definition := executionDefinition("scheduled-plan:one-window", schedulersdk.Policy{})
	if err := first.ReconcileScheduledPlan(t.Context(), definition, dueAt, true); err != nil {
		t.Fatal(err)
	}
	itemsA, err := first.Due(t.Context(), time.Now().UTC(), 1)
	if err != nil || len(itemsA) != 1 {
		t.Fatalf("first due=%+v err=%v", itemsA, err)
	}
	itemsB, err := second.Due(t.Context(), time.Now().UTC(), 1)
	if err != nil || len(itemsB) != 1 {
		t.Fatalf("second due=%+v err=%v", itemsB, err)
	}
	var wait sync.WaitGroup
	claimed := make(chan bool, 2)
	errs := make(chan error, 2)
	for _, input := range []struct {
		store *Store
		item  modulehost.DueTrigger
	}{{first, itemsA[0]}, {second, itemsB[0]}} {
		wait.Add(1)
		go func(input struct {
			store *Store
			item  modulehost.DueTrigger
		}) {
			defer wait.Done()
			_, ok, claimErr := input.store.Claim(t.Context(), input.item, time.Minute)
			claimed <- ok
			errs <- claimErr
		}(input)
	}
	wait.Wait()
	close(claimed)
	close(errs)
	wins := 0
	for ok := range claimed {
		if ok {
			wins++
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var rows int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _scheduler_runs WHERE runtime_id = ? AND definition_key = ?`, "runtime-plan", definition.Key).Scan(&rows); err != nil || wins != 1 || rows != 1 {
		t.Fatalf("wins=%d rows=%d err=%v", wins, rows, err)
	}
}

func TestExpiredScheduledPlanLeaseIsRecoveredAfterRestart(t *testing.T) {
	db := openScheduledPlanExecutionDatabase(t, "expired-lease")
	defer db.Close()
	dialect, _ := Renderer("sqlite", "")
	first, _ := NewStore(db, dialect, "runtime-plan", "worker-before")
	dueAt := time.Now().UTC().Add(-time.Second)
	definition := executionDefinition("scheduled-plan:restart", schedulersdk.Policy{MaxAttempts: 3})
	definition.Schedule = schedulersdk.Schedule{Type: "once", Expression: dueAt.Format(time.RFC3339Nano), Timezone: "UTC"}
	if err := first.ReconcileScheduledPlan(t.Context(), definition, dueAt, true); err != nil {
		t.Fatal(err)
	}
	due, err := first.Due(t.Context(), time.Now().UTC(), 1)
	if err != nil || len(due) != 1 {
		t.Fatalf("due=%+v err=%v", due, err)
	}
	run, claimed, err := first.Claim(t.Context(), due[0], time.Minute)
	if err != nil || !claimed {
		t.Fatalf("run=%+v claimed=%t err=%v", run, claimed, err)
	}
	if _, err := db.ExecContext(t.Context(), `UPDATE _scheduler_runs SET lease_expires_at = ? WHERE runtime_id = ? AND run_id = ?`, time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano), "runtime-plan", run.Trigger.RunID); err != nil {
		t.Fatal(err)
	}
	second, _ := NewStore(db, dialect, "runtime-plan", "worker-after")
	recovered, err := second.Due(t.Context(), time.Now().UTC(), 1)
	if err != nil || len(recovered) != 1 || !recovered[0].ExpectedCursor.IsZero() || recovered[0].ScheduledFor != due[0].ScheduledFor {
		t.Fatalf("recovered=%+v err=%v", recovered, err)
	}
	taken, claimed, err := second.Claim(t.Context(), recovered[0], time.Minute)
	if err != nil || !claimed || taken.Lease.Owner != "worker-after" || taken.Lease.Token != 2 || taken.Trigger.Attempt != 2 {
		t.Fatalf("taken=%+v claimed=%t err=%v", taken, claimed, err)
	}
}

func openScheduledPlanExecutionDatabase(t *testing.T, suffix string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:scheduler-plan-execution-"+suffix+"?mode=memory&cache=shared&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	if err := EnsureSchema(t.Context(), db, "sqlite", ""); err != nil {
		db.Close()
		t.Fatal(err)
	}
	return db
}

func executionDefinition(key string, policy schedulersdk.Policy) schedulersdk.Definition {
	return schedulersdk.Definition{
		Key: key, Name: key, Status: "enabled", Revision: "1",
		Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60, Timezone: "UTC"},
		Target:   schedulersdk.TargetRef{Type: "runtime_operation", Owner: "agent", Operation: "conversation_task_start"},
		Policy:   policy,
	}
}

func weekdayCalendarProtocol() *schedulersdk.BusinessCalendarSnapshot {
	days := []string{"monday", "tuesday", "wednesday", "thursday", "friday"}
	weekly := make([]schedulersdk.BusinessCalendarWeeklySchedule, 0, len(days))
	for _, day := range days {
		weekly = append(weekly, schedulersdk.BusinessCalendarWeeklySchedule{Weekday: day, Intervals: []schedulersdk.BusinessCalendarTimeInterval{{Start: "09:00", End: "18:00"}}})
	}
	return &schedulersdk.BusinessCalendarSnapshot{Key: "weekday", Revision: "1", Timezone: "UTC", WeeklyWorkingIntervals: weekly}
}

func assertDefinitionCursor(t *testing.T, db *sql.DB, key string, enabled bool, want time.Time, status, source string) {
	t.Helper()
	var raw, gotStatus, gotSource string
	var gotEnabled bool
	if err := db.QueryRowContext(t.Context(), `SELECT enabled, next_run_at, last_run_status, source_kind FROM _scheduler_schedules WHERE runtime_id = ? AND schedule_id = ?`, "runtime-plan", key).Scan(&gotEnabled, &raw, &gotStatus, &gotSource); err != nil {
		t.Fatal(err)
	}
	got, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil || gotEnabled != enabled || !got.Equal(want) || gotStatus != status || gotSource != source {
		t.Fatalf("key=%s enabled=%t cursor=%s status=%q source=%q parse=%v", key, gotEnabled, got, gotStatus, gotSource, err)
	}
}
