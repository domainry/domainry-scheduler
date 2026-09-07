package module

import (
	"context"
	"database/sql"
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
func (*restartModuleHost) Dispatch(_ context.Context, trigger schedulersdk.Trigger) (schedulersdk.DownstreamReceipt, error) {
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

var _ modulehost.ModuleHost = (*restartModuleHost)(nil)
