package database

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulerpersistence "github.com/domainry/domainry-scheduler-sdk/persistence"
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
	otherReceipt, claimed, err := otherRuntime.ClaimCommand(t.Context(), claim)
	if err != nil || !claimed || otherReceipt.Status != schedulermodel.CommandReceiptExecuting || otherReceipt.HTTPStatus != 0 || len(otherReceipt.ResponseJSON) != 0 {
		t.Fatalf("other runtime receipt=%+v claimed=%t err=%v", otherReceipt, claimed, err)
	}
}

func TestDefinitionStoreScopesSameAndDifferentKeysByRuntimeAcrossReopen(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-definition-runtime-scope?mode=memory&cache=shared")
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
	runtimeA, err := NewDefinitionStore(db, dialect, "runtime-a", schedulersdk.DeploymentModeModule)
	if err != nil {
		t.Fatal(err)
	}
	runtimeB, err := NewDefinitionStore(db, dialect, "runtime-b", schedulersdk.DeploymentModeModule)
	if err != nil {
		t.Fatal(err)
	}
	aDefinitions := []schedulersdk.Definition{
		testDefinition("shared", "run-a", `{"tenant":"a"}`),
		testDefinition("a-only", "only-a", `{"tenant":"a-only"}`),
	}
	bDefinitions := []schedulersdk.Definition{
		testDefinition("shared", "run-b", `{"tenant":"b"}`),
		testDefinition("b-only", "only-b", `{"tenant":"b-only"}`),
	}
	if err := runtimeA.SyncDefinitions(t.Context(), schedulerpersistence.DefinitionSnapshot{Revision: 1, SchemaVersion: "1", SourceKind: "runtime_host", SourceID: "runtime-a", Definitions: aDefinitions}); err != nil {
		t.Fatal(err)
	}
	if err := runtimeB.SyncDefinitions(t.Context(), schedulerpersistence.DefinitionSnapshot{Revision: 2, SchemaVersion: "2", SourceKind: "runtime_host", SourceID: "runtime-b", Definitions: bDefinitions}); err != nil {
		t.Fatal(err)
	}
	assertDefinitionSnapshot(t, runtimeA, "runtime-a", map[string]string{"shared": "run-a", "a-only": "only-a"})
	assertDefinitionSnapshot(t, runtimeB, "runtime-b", map[string]string{"shared": "run-b", "b-only": "only-b"})

	reopenedA, err := NewDefinitionStore(db, dialect, "runtime-a", schedulersdk.DeploymentModeModule)
	if err != nil {
		t.Fatal(err)
	}
	reopenedB, err := NewDefinitionStore(db, dialect, "runtime-b", schedulersdk.DeploymentModeModule)
	if err != nil {
		t.Fatal(err)
	}
	assertDefinitionSnapshot(t, reopenedA, "runtime-a", map[string]string{"shared": "run-a", "a-only": "only-a"})
	assertDefinitionSnapshot(t, reopenedB, "runtime-b", map[string]string{"shared": "run-b", "b-only": "only-b"})

	if err := reopenedB.SyncDefinitions(t.Context(), schedulerpersistence.DefinitionSnapshot{Revision: 3, SchemaVersion: "3", SourceKind: "runtime_host", SourceID: "runtime-a", Definitions: aDefinitions}); err == nil {
		t.Fatal("runtime-b store accepted runtime-a snapshot")
	}
	assertDefinitionSnapshot(t, reopenedA, "runtime-a", map[string]string{"shared": "run-a", "a-only": "only-a"})
	assertDefinitionSnapshot(t, reopenedB, "runtime-b", map[string]string{"shared": "run-b", "b-only": "only-b"})

	var activeRows, storageKeys int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*), COUNT(DISTINCT resource_key) FROM _scheduler_definitions WHERE disabled_at IS NULL`).Scan(&activeRows, &storageKeys); err != nil {
		t.Fatal(err)
	}
	if activeRows != 4 || storageKeys != 4 {
		t.Fatalf("active definition rows=%d distinct storage keys=%d", activeRows, storageKeys)
	}
}

func TestDefinitionStoreAcceptsChangedSnapshotFromNewProcessLocalRevisionEpoch(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-definition-process-epochs?mode=memory&cache=shared")
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
	store, err := NewDefinitionStore(db, dialect, "runtime-a", schedulersdk.DeploymentModeModule)
	if err != nil {
		t.Fatal(err)
	}
	current := testDefinition("shared", "revision-7", `{"revision":7}`)
	if err := store.SyncDefinitions(t.Context(), schedulerpersistence.DefinitionSnapshot{Revision: 7, SchemaVersion: "7", SourceKind: "runtime_host", SourceID: "runtime-a", Definitions: []schedulersdk.Definition{current}}); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewDefinitionStore(db, dialect, "runtime-a", schedulersdk.DeploymentModeModule)
	if err != nil {
		t.Fatal(err)
	}
	restarted := testDefinition("shared", "changed-after-restart", `{"revision":1,"runtime_restart":true}`)
	if err := reopened.SyncDefinitions(t.Context(), schedulerpersistence.DefinitionSnapshot{Revision: 1, SchemaVersion: "1", SourceKind: "runtime_host", SourceID: "runtime-a", Definitions: []schedulersdk.Definition{restarted}}); err != nil {
		t.Fatalf("new Runtime process revision 1 err=%v", err)
	}
	snapshot, err := reopened.DefinitionSnapshot(t.Context())
	if err != nil || snapshot.Revision != 1 || len(snapshot.Definitions) != 1 || snapshot.Definitions[0].Target.Operation != "changed-after-restart" {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
}

func TestDefinitionStoreEnforcesDeploymentPublicationBoundary(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-definition-mode-boundary?mode=memory&cache=shared")
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
	moduleStore, err := NewDefinitionStore(db, dialect, "runtime-module", schedulersdk.DeploymentModeModule)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := moduleStore.BeginDefinitionPublisherSession(t.Context()); !errors.Is(err, schedulersdk.ErrDefinitionPublicationCapabilityRequired) {
		t.Fatalf("Module begin session err=%v", err)
	}
	fence := schedulersdk.DefinitionPublisherFence{ContractVersion: schedulersdk.DefinitionPublicationContractVersion, Generation: 1, SessionSHA256: strings.Repeat("a", 64)}
	if err := moduleStore.SyncDefinitions(t.Context(), schedulerpersistence.DefinitionSnapshot{
		PublisherFence: &fence, Revision: 1, SchemaVersion: "1", SourceKind: "runtime_host", SourceID: "runtime-module", Definitions: []schedulersdk.Definition{testDefinition("daily", "module", `{}`)},
	}); err == nil {
		t.Fatal("Module store accepted a SaaS publisher fence")
	}
	saasStore, err := NewDefinitionStore(db, dialect, "runtime-saas", schedulersdk.DeploymentModeSaaS)
	if err != nil {
		t.Fatal(err)
	}
	if err := saasStore.SyncDefinitions(t.Context(), schedulerpersistence.DefinitionSnapshot{
		Revision: 1, SchemaVersion: "1", SourceKind: "runtime_host", SourceID: "runtime-saas", Definitions: []schedulersdk.Definition{testDefinition("daily", "saas", `{}`)},
	}); !errors.Is(err, schedulersdk.ErrDefinitionPublicationRequired) {
		t.Fatalf("SaaS nil fence err=%v", err)
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO _scheduler_definition_publications (
		source_kind, source_id, active_generation, active_session_sha256, updated_at
	) VALUES (?, ?, ?, ?, ?)`, "runtime_host", "runtime-limit", int64(1<<53-1), strings.Repeat("b", 64), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	limitStore, err := NewDefinitionStore(db, dialect, "runtime-limit", schedulersdk.DeploymentModeSaaS)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := limitStore.BeginDefinitionPublisherSession(t.Context()); err == nil || !strings.Contains(err.Error(), "generation exhausted") {
		t.Fatalf("generation limit err=%v", err)
	}
}

func testDefinition(key, operation, payload string) schedulersdk.Definition {
	return schedulersdk.Definition{
		Key: key, Name: key, Revision: "v1", Status: "enabled",
		Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60},
		Target:   schedulersdk.TargetRef{Type: "runtime_operation", Owner: "workflow", Operation: operation, Payload: json.RawMessage(payload)},
	}
}

func assertDefinitionSnapshot(t *testing.T, store DefinitionStore, runtimeID string, expected map[string]string) {
	t.Helper()
	snapshot, err := store.DefinitionSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.SourceKind != "runtime_host" || snapshot.SourceID != runtimeID || len(snapshot.Definitions) != len(expected) {
		t.Fatalf("runtime=%s snapshot=%+v", runtimeID, snapshot)
	}
	for _, definition := range snapshot.Definitions {
		if operation, found := expected[definition.Key]; !found || definition.Target.Operation != operation {
			t.Fatalf("runtime=%s leaked or overwritten definition=%+v expected=%v", runtimeID, definition, expected)
		}
	}
}

func TestOrdinaryClaimKeepsFractionalScheduleButUsesLegacySecondPrecisionWindow(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-ordinary-fractional-window?mode=memory&cache=shared")
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
	definition := schedulersdk.Definition{Key: "fractional", Revision: "v1", Status: "enabled", Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60}, Target: schedulersdk.TargetRef{Type: "http", ConnectionKey: "downstream", Operation: "sync", DispatchMode: "direct"}}
	scheduledFor := time.Date(2026, 9, 7, 9, 30, 15, 987654321, time.FixedZone("offset", 8*60*60))
	run, claimed, err := store.Claim(t.Context(), modulehost.DueTrigger{Definition: definition, ScheduledFor: scheduledFor}, time.Minute)
	if err != nil || !claimed {
		t.Fatalf("run=%+v claimed=%t err=%v", run, claimed, err)
	}
	wantWindow := scheduledFor.UTC().Format(time.RFC3339)
	if !run.Trigger.ScheduledFor.Equal(scheduledFor) || run.Trigger.WindowKey != wantWindow || run.Trigger.WindowKey == scheduledFor.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("run=%+v want window=%q", run, wantWindow)
	}
	persisted, err := store.Get(t.Context(), run.Trigger.RunID)
	if err != nil || !persisted.Trigger.ScheduledFor.Equal(scheduledFor) || persisted.Trigger.WindowKey != wantWindow {
		t.Fatalf("persisted=%+v err=%v", persisted, err)
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
	if count != 7 {
		t.Fatalf("applied migrations=%d", count)
	}
	var definitions int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '_scheduler_definitions'`).Scan(&definitions); err != nil {
		t.Fatal(err)
	}
	if definitions != 1 {
		t.Fatalf("scheduler definition tables=%d", definitions)
	}
	var plans int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '_scheduler_plans'`).Scan(&plans); err != nil {
		t.Fatal(err)
	}
	if plans != 1 {
		t.Fatalf("scheduler plan tables=%d", plans)
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
