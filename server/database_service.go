package server

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulerstore "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence"
	schedulermodule "github.com/domainry/domainry-scheduler/module"
)

type DownstreamHost interface {
	modulehost.Dispatcher
	modulehost.HTTPConnectionProvider
}

type DatabaseServiceOptions struct {
	Database    *sql.DB
	Driver      string
	Schema      string
	WorkerID    string
	Worker      schedulersdk.WorkerConfig
	Downstreams func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error)
}

// DatabaseService is the production SaaS implementation behind the HTTP
// protocol. Every application has an isolated binding and runtime_id row
// scope while all bindings share the Scheduler-owned service database.
type DatabaseService struct {
	options      DatabaseServiceOptions
	dialect      modulehost.Dialect
	mu           sync.Mutex
	applications map[string]*databaseApplication
}

type databaseApplication struct {
	mu       sync.RWMutex
	snapshot schedulersdk.DefinitionSnapshot
	binding  schedulersdk.Binding
	host     *databaseApplicationHost
	cancel   context.CancelFunc
	done     <-chan struct{}
}

type databaseApplicationHost struct {
	service     DatabaseServiceOptions
	dialect     modulehost.Dialect
	application *databaseApplication
	downstream  DownstreamHost
}

func NewDatabaseService(options DatabaseServiceOptions) (*DatabaseService, error) {
	if options.Database == nil || strings.TrimSpace(options.Driver) == "" || strings.TrimSpace(options.WorkerID) == "" || options.Downstreams == nil {
		return nil, fmt.Errorf("Scheduler SaaS database service is incomplete")
	}
	dialect, err := schedulerstore.Renderer(options.Driver, options.Schema)
	if err != nil {
		return nil, fmt.Errorf("Scheduler SaaS database dialect: %w", err)
	}
	return &DatabaseService{options: options, dialect: dialect, applications: map[string]*databaseApplication{}}, nil
}

func (s *DatabaseService) application(ctx context.Context, ref schedulersdk.ApplicationRef) (*databaseApplication, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	key := strings.TrimSpace(ref.RuntimeID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if value := s.applications[key]; value != nil {
		return value, nil
	}
	downstream, err := s.options.Downstreams(ctx, ref)
	if err != nil {
		return nil, err
	}
	if downstream == nil {
		return nil, fmt.Errorf("Scheduler SaaS downstream host is unavailable for %s", key)
	}
	state := &databaseApplication{}
	host := &databaseApplicationHost{service: s.options, dialect: s.dialect, application: state, downstream: downstream}
	state.host = host
	binding, err := schedulermodule.NewFactory(schedulermodule.Options{}).OpenSaaSApplication(ctx, ref, host)
	if err != nil {
		return nil, err
	}
	state.binding = binding
	worker := s.options.Worker
	if worker == (schedulersdk.WorkerConfig{}) {
		worker.Enabled = true
	}
	workerCtx, cancel := context.WithCancel(context.Background())
	state.cancel = cancel
	state.done = binding.Start(workerCtx, worker)
	s.applications[key] = state
	return state, nil
}

func (s *DatabaseService) Descriptor(context.Context, schedulersdk.ApplicationRef) (schedulersdk.Descriptor, error) {
	return schedulersdk.Descriptor{ProtocolVersion: schedulersdk.ProtocolVersionV1, Mode: schedulersdk.DeploymentModeSaaS, Capabilities: []string{"configuration_reconcile", "schedule_preview", "durable_trigger", "manual_trigger", "run_evidence"}}, nil
}
func (s *DatabaseService) Reconcile(ctx context.Context, ref schedulersdk.ApplicationRef, snapshot schedulersdk.DefinitionSnapshot) error {
	app, err := s.application(ctx, ref)
	if err != nil {
		return err
	}
	app.mu.Lock()
	app.snapshot = cloneSnapshot(snapshot)
	app.mu.Unlock()
	return app.binding.Reconcile(ctx)
}
func (s *DatabaseService) Preview(ctx context.Context, ref schedulersdk.ApplicationRef, value schedulersdk.Schedule, after time.Time, count int) ([]time.Time, error) {
	app, err := s.application(ctx, ref)
	if err != nil {
		return nil, err
	}
	return app.binding.Preview(ctx, value, after, count)
}
func (s *DatabaseService) Tick(ctx context.Context, ref schedulersdk.ApplicationRef, now time.Time, limit int) (int, error) {
	app, err := s.application(ctx, ref)
	if err != nil {
		return 0, err
	}
	return app.binding.Tick(ctx, now, limit)
}
func (s *DatabaseService) TriggerNow(ctx context.Context, ref schedulersdk.ApplicationRef, key, reason string) (schedulersdk.Run, error) {
	app, err := s.application(ctx, ref)
	if err != nil {
		return schedulersdk.Run{}, err
	}
	return app.binding.TriggerNow(ctx, key, reason)
}
func (s *DatabaseService) Reschedule(ctx context.Context, ref schedulersdk.ApplicationRef, key string, nextRunAt time.Time, reason string) error {
	binding, err := s.binding(ctx, ref)
	if err != nil {
		return err
	}
	return binding.Reschedule(ctx, key, nextRunAt, reason)
}
func (s *DatabaseService) Runs(ctx context.Context, ref schedulersdk.ApplicationRef, limit int) ([]schedulersdk.Run, error) {
	app, err := s.application(ctx, ref)
	if err != nil {
		return nil, err
	}
	return app.binding.Runs(ctx, limit)
}

func (s *DatabaseService) binding(ctx context.Context, ref schedulersdk.ApplicationRef) (schedulersdk.Binding, error) {
	app, err := s.application(ctx, ref)
	if err != nil {
		return nil, err
	}
	return app.binding, nil
}

func (s *DatabaseService) Run(ctx context.Context, ref schedulersdk.ApplicationRef, id string) (schedulersdk.Run, error) {
	binding, err := s.binding(ctx, ref)
	if err != nil {
		return schedulersdk.Run{}, err
	}
	return binding.Run(ctx, id)
}
func (s *DatabaseService) RetryRun(ctx context.Context, ref schedulersdk.ApplicationRef, id, reason string) (schedulersdk.Run, error) {
	binding, err := s.binding(ctx, ref)
	if err != nil {
		return schedulersdk.Run{}, err
	}
	return binding.RetryRun(ctx, id, reason)
}
func (s *DatabaseService) CancelRun(ctx context.Context, ref schedulersdk.ApplicationRef, id, reason string) (schedulersdk.Run, error) {
	binding, err := s.binding(ctx, ref)
	if err != nil {
		return schedulersdk.Run{}, err
	}
	return binding.CancelRun(ctx, id, reason)
}
func (s *DatabaseService) DeadLetter(ctx context.Context, ref schedulersdk.ApplicationRef, id string) (schedulersdk.DeadLetter, error) {
	binding, err := s.binding(ctx, ref)
	if err != nil {
		return schedulersdk.DeadLetter{}, err
	}
	return binding.DeadLetter(ctx, id)
}
func (s *DatabaseService) ResolveDeadLetter(ctx context.Context, ref schedulersdk.ApplicationRef, id, reason string) (schedulersdk.DeadLetter, error) {
	binding, err := s.binding(ctx, ref)
	if err != nil {
		return schedulersdk.DeadLetter{}, err
	}
	return binding.ResolveDeadLetter(ctx, id, reason)
}
func (s *DatabaseService) RequeueDeadLetter(ctx context.Context, ref schedulersdk.ApplicationRef, id, reason string) (schedulersdk.Run, error) {
	binding, err := s.binding(ctx, ref)
	if err != nil {
		return schedulersdk.Run{}, err
	}
	return binding.RequeueDeadLetter(ctx, id, reason)
}
func (s *DatabaseService) Close(ctx context.Context, ref schedulersdk.ApplicationRef) error {
	key := strings.TrimSpace(ref.RuntimeID)
	s.mu.Lock()
	app := s.applications[key]
	delete(s.applications, key)
	s.mu.Unlock()
	if app == nil {
		return nil
	}
	if app.cancel != nil {
		app.cancel()
	}
	return app.binding.Close(ctx)
}

func (h *databaseApplicationHost) Definitions() modulehost.DefinitionProvider { return h }
func (h *databaseApplicationHost) Dispatcher() modulehost.Dispatcher          { return h.downstream }
func (h *databaseApplicationHost) HTTPConnections() modulehost.HTTPConnectionProvider {
	return h.downstream
}
func (h *databaseApplicationHost) Database() modulehost.Database { return h.service.Database }
func (h *databaseApplicationHost) Dialect() modulehost.Dialect   { return h.dialect }
func (h *databaseApplicationHost) Driver() string                { return h.service.Driver }
func (h *databaseApplicationHost) Schema() string                { return h.service.Schema }
func (h *databaseApplicationHost) WorkerID() string              { return h.service.WorkerID }
func (h *databaseApplicationHost) Snapshot(context.Context) (schedulersdk.DefinitionSnapshot, error) {
	h.application.mu.RLock()
	defer h.application.mu.RUnlock()
	return cloneSnapshot(h.application.snapshot), nil
}

func cloneSnapshot(value schedulersdk.DefinitionSnapshot) schedulersdk.DefinitionSnapshot {
	out := value
	out.Definitions = append([]schedulersdk.Definition(nil), value.Definitions...)
	return out
}

var _ Service = (*DatabaseService)(nil)
var _ schedulermodule.SaaSHost = (*databaseApplicationHost)(nil)
