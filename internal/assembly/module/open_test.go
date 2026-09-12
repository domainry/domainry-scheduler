package module

import (
	"context"
	"database/sql"
	"encoding/json"
	"runtime"
	"sync"
	"testing"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulerpersistence "github.com/domainry/domainry-scheduler-sdk/persistence"
	schedulerstore "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence/database"
	_ "modernc.org/sqlite"
)

type restartMigrationRegistrar struct {
	mu      sync.Mutex
	db      *sql.DB
	applied bool
}

func (*restartMigrationRegistrar) Driver() string { return "sqlite" }
func (*restartMigrationRegistrar) Schema() string { return "" }
func (r *restartMigrationRegistrar) ApplyOwnedMigrations(ctx context.Context, _ string, migrations []modulehost.SchemaMigration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.applied {
		return nil
	}
	for _, migration := range migrations {
		for _, statement := range migration.Statements {
			if _, err := r.db.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
	}
	r.applied = true
	return nil
}

type restartModuleHost struct {
	db         *sql.DB
	dialect    modulehost.Dialect
	migrations *restartMigrationRegistrar
	definition schedulersdk.Definition
	mu         sync.Mutex
	revision   int64
	dispatch   chan schedulersdk.Trigger
}

func (h *restartModuleHost) Definitions() modulehost.DefinitionProvider { return h }
func (h *restartModuleHost) Dispatcher() modulehost.Dispatcher          { return h }
func (h *restartModuleHost) HTTPConnections() modulehost.HTTPConnectionProvider {
	return h
}
func (h *restartModuleHost) Database() modulehost.Database             { return h.db }
func (h *restartModuleHost) Dialect() modulehost.Dialect               { return h.dialect }
func (h *restartModuleHost) Migrations() modulehost.MigrationRegistrar { return h.migrations }
func (*restartModuleHost) WorkerID() string                            { return "module-restart-worker" }
func (h *restartModuleHost) Snapshot(context.Context) (schedulersdk.DefinitionSnapshot, error) {
	h.mu.Lock()
	h.revision++
	revision := h.revision
	h.mu.Unlock()
	return schedulersdk.DefinitionSnapshot{Revision: revision, Definitions: []schedulersdk.Definition{h.definition}}, nil
}
func (h *restartModuleHost) Dispatch(_ context.Context, trigger schedulersdk.Trigger) (schedulersdk.DownstreamReceipt, error) {
	if h.dispatch != nil {
		select {
		case h.dispatch <- trigger:
		default:
		}
	}
	return schedulersdk.DownstreamReceipt{ID: "receipt-" + trigger.RunID, Owner: trigger.Target.Owner, Status: "accepted"}, nil
}
func (*restartModuleHost) ResolveHTTPConnection(context.Context, string) (modulehost.HTTPConnection, error) {
	return modulehost.HTTPConnection{}, nil
}

func TestModuleRestartAcceptsNewProcessLocalRevisionEpoch(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-module-revision-restart?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dialect, err := schedulerstore.Renderer("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	registrar := &restartMigrationRegistrar{db: db}
	application := schedulersdk.ApplicationRef{RuntimeID: "runtime-module-restart"}
	definition := schedulersdk.Definition{
		Key: "daily", Name: "Daily", Revision: "v1", Status: "enabled",
		Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60},
		Target:   schedulersdk.TargetRef{Type: "runtime_operation", Owner: "workflow", Operation: "run"},
	}
	openProcess := func(processDefinition schedulersdk.Definition) (schedulersdk.Binding, *restartModuleHost) {
		host := &restartModuleHost{db: db, dialect: dialect, migrations: registrar, definition: processDefinition}
		binding, openErr := OpenModule(t.Context(), application, host)
		if openErr != nil {
			t.Fatal(openErr)
		}
		return binding, host
	}
	waitRevision := func(binding schedulersdk.Binding, want int64) {
		t.Helper()
		repository := binding.(schedulerpersistence.Binding).DefinitionRepository()
		for {
			snapshot, readErr := repository.DefinitionSnapshot(t.Context())
			if readErr != nil {
				t.Fatal(readErr)
			}
			if snapshot.Revision == want {
				return
			}
			runtime.Gosched()
		}
	}

	first, _ := openProcess(definition)
	if err := first.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	firstWorker, cancelFirst := context.WithCancel(t.Context())
	firstDone := first.Start(firstWorker, schedulersdk.WorkerConfig{Enabled: true, PollInterval: time.Hour, BatchSize: 1})
	waitRevision(first, 2)
	cancelFirst()
	<-firstDone
	if err := first.Close(t.Context()); err != nil {
		t.Fatal(err)
	}

	restartedDefinition := definition
	restartedDefinition.Target.Operation = "run-after-restart"
	second, _ := openProcess(restartedDefinition)
	if err := second.Reconcile(t.Context()); err != nil {
		t.Fatalf("new Module process revision 1 was rejected after durable revision 2: %v", err)
	}
	// Module revisions are process-local. A new Module process may therefore
	// publish changed content beginning again at revision 1.
	waitRevision(second, 1)
	repository := second.(schedulerpersistence.Binding).DefinitionRepository()
	snapshot, err := repository.DefinitionSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Definitions) != 1 || snapshot.Definitions[0].Target.Operation != "run-after-restart" {
		t.Fatalf("restarted Module content=%+v", snapshot.Definitions)
	}
	secondWorker, cancelSecond := context.WithCancel(t.Context())
	secondDone := second.Start(secondWorker, schedulersdk.WorkerConfig{Enabled: true, PollInterval: time.Hour, BatchSize: 1})
	waitRevision(second, 2)
	cancelSecond()
	<-secondDone
	if err := second.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestModuleScheduledPlansPersistOneTimeAndRecurringRecordsAcrossRestart(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-module-plans?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dialect, err := schedulerstore.Renderer("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	registrar := &restartMigrationRegistrar{db: db}
	application := schedulersdk.ApplicationRef{RuntimeID: "runtime-plan"}
	open := func() schedulersdk.Binding {
		host := &restartModuleHost{db: db, dialect: dialect, migrations: registrar, definition: schedulersdk.Definition{Key: "unused", Revision: "v1", Status: "disabled", Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60}, Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "agent", Operation: "unused"}}}
		binding, openErr := OpenModule(t.Context(), application, host)
		if openErr != nil {
			t.Fatal(openErr)
		}
		return binding
	}
	owner := schedulersdk.ScheduledPlanOwner{WorkspaceID: "workspace-a", UserID: "user-a", ProductKey: "agent"}
	at := time.Date(2026, 9, 18, 1, 30, 0, 0, time.UTC)
	inputs := []schedulersdk.ScheduledPlanCreate{
		{ClientID: "once", Name: "周五提醒", Owner: owner, Timezone: "Asia/Shanghai", Trigger: schedulersdk.ScheduledPlanTrigger{Type: "once", At: &at}, Input: json.RawMessage(`{"goal":"提醒未完成事项"}`), AllowedActions: []string{"todo.list"}, Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "agent", Operation: "conversation_task_start"}, ConversationRef: schedulersdk.ScheduledPlanConversationRef{ConversationID: "conversation-a", RunID: "run-a"}},
		{ClientID: "weekly", Name: "每周整理", Owner: owner, Timezone: "Asia/Shanghai", Trigger: schedulersdk.ScheduledPlanTrigger{Type: "recurring", Schedule: &schedulersdk.Schedule{Type: "weekly_at", TimeOfDay: "09:00", DayOfWeek: "monday", Timezone: "Asia/Shanghai"}}, Input: json.RawMessage(`{"goal":"整理本周待办"}`), AllowedActions: []string{"artifact.create", "todo.list"}, Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "agent", Operation: "conversation_task_start"}, ConversationRef: schedulersdk.ScheduledPlanConversationRef{ConversationID: "conversation-a"}},
	}
	first := open()
	plans := first.(schedulersdk.ScheduledPlanService)
	receipts := make([]schedulersdk.ScheduledPlanReceipt, 0, len(inputs))
	for _, input := range inputs {
		receipt, createErr := plans.CreateScheduledPlan(t.Context(), input)
		if createErr != nil || receipt.Replay || receipt.Plan.Revision != 1 {
			t.Fatalf("receipt=%+v err=%v", receipt, createErr)
		}
		receipts = append(receipts, receipt)
	}
	if err := first.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	second := open()
	defer second.Close(t.Context())
	reopened := second.(schedulersdk.ScheduledPlanService)
	for index, receipt := range receipts {
		loaded, getErr := reopened.GetScheduledPlan(t.Context(), schedulersdk.ScheduledPlanLookup{Owner: owner, PlanID: receipt.Plan.ID})
		if getErr != nil || loaded.ID != receipt.Plan.ID || loaded.Trigger.Type != inputs[index].Trigger.Type || loaded.Timezone != "Asia/Shanghai" || loaded.ConversationRef.ConversationID != "conversation-a" {
			t.Fatalf("index=%d loaded=%+v err=%v", index, loaded, getErr)
		}
	}
}

func TestModuleScheduledPlanClockDispatchesOnceAndRestartDoesNotDuplicate(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-module-plan-clock?mode=memory&cache=shared&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dialect, err := schedulerstore.Renderer("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	registrar := &restartMigrationRegistrar{db: db}
	application := schedulersdk.ApplicationRef{RuntimeID: "runtime-module-plan-clock"}
	dispatched := make(chan schedulersdk.Trigger, 2)
	open := func() (schedulersdk.Binding, context.CancelFunc, <-chan struct{}) {
		host := &restartModuleHost{db: db, dialect: dialect, migrations: registrar, dispatch: dispatched, definition: schedulersdk.Definition{Key: "unused", Revision: "v1", Status: "disabled", Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60}, Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "agent", Operation: "unused"}}}
		binding, openErr := OpenModule(t.Context(), application, host)
		if openErr != nil {
			t.Fatal(openErr)
		}
		worker, cancel := context.WithCancel(t.Context())
		done := binding.Start(worker, schedulersdk.WorkerConfig{Enabled: true, PollInterval: 5 * time.Millisecond, BatchSize: 5, LeaseTTL: time.Second})
		return binding, cancel, done
	}

	first, cancelFirst, firstDone := open()
	dueAt := time.Now().UTC().Add(30 * time.Millisecond)
	owner := schedulersdk.ScheduledPlanOwner{WorkspaceID: "workspace-a", UserID: "user-a", ProductKey: "agent"}
	receipt, err := first.(schedulersdk.ScheduledPlanService).CreateScheduledPlan(t.Context(), schedulersdk.ScheduledPlanCreate{
		ClientID: "one-time-module", Name: "一次提醒", Owner: owner, Timezone: "Asia/Shanghai",
		Trigger: schedulersdk.ScheduledPlanTrigger{Type: schedulersdk.ScheduledPlanTriggerOnce, At: &dueAt},
		Input:   json.RawMessage(`{"goal":"提醒一次"}`), AllowedActions: []string{"todo.list"},
		Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "agent", Operation: "conversation_task_start"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var trigger schedulersdk.Trigger
	select {
	case trigger = <-dispatched:
	case <-time.After(2 * time.Second):
		t.Fatal("Module plan clock did not dispatch")
	}
	var payload schedulersdk.ScheduledPlanDispatch
	if err := json.Unmarshal(trigger.Target.Payload, &payload); err != nil || payload.PlanID != receipt.Plan.ID || payload.Owner != owner || string(payload.Input) != `{"goal":"提醒一次"}` {
		t.Fatalf("payload=%+v raw=%s err=%v", payload, trigger.Target.Payload, err)
	}
	cancelFirst()
	<-firstDone
	if err := first.Close(t.Context()); err != nil {
		t.Fatal(err)
	}

	second, cancelSecond, secondDone := open()
	defer func() {
		cancelSecond()
		<-secondDone
		_ = second.Close(t.Context())
	}()
	select {
	case duplicate := <-dispatched:
		t.Fatalf("one-time Module plan duplicated after restart: %+v", duplicate)
	case <-time.After(75 * time.Millisecond):
	}
	var runs int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _scheduler_runs WHERE runtime_id = ? AND definition_key = ?`, application.RuntimeID, "scheduled-plan:"+receipt.Plan.ID).Scan(&runs); err != nil || runs != 1 {
		t.Fatalf("runs=%d err=%v", runs, err)
	}
}

var _ modulehost.ModuleHost = (*restartModuleHost)(nil)
