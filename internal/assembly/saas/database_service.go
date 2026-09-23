package saas

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulerpersistence "github.com/domainry/domainry-scheduler-sdk/persistence"
	schedulersdkadapter "github.com/domainry/domainry-scheduler/internal/adapter/schedulersdk"
	moduleassembly "github.com/domainry/domainry-scheduler/internal/assembly/module"
	schedulerstore "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence"
	schedulermigration "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence/database/migration"
)

type DownstreamHost = schedulersdkadapter.DownstreamHost

var (
	ErrStaleDefinitionSnapshot       = schedulersdk.ErrDefinitionSnapshotStale
	ErrConflictingDefinitionSnapshot = schedulersdk.ErrDefinitionSnapshotConflict
)

type DatabaseServiceOptions struct {
	Context      context.Context
	Database     *sql.DB
	Driver       string
	Schema       string
	WorkerID     string
	Worker       schedulersdk.WorkerConfig
	Applications []schedulersdk.ApplicationRef
	Downstreams  func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error)
}

// DatabaseService is the production SaaS implementation behind the HTTP
// protocol. Every application has an isolated binding and runtime_id row
// scope while all bindings share the Scheduler-owned service database.
type DatabaseService struct {
	options      DatabaseServiceOptions
	dialect      modulehost.Dialect
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	applications map[string]*databaseApplication
	configured   map[string]bool
}

type databaseApplication struct {
	mu          sync.RWMutex
	reconcileMu sync.Mutex
	ready       bool
	binding     schedulersdk.Binding
	host        *databaseApplicationHost
	cancel      context.CancelFunc
	done        <-chan struct{}
}

type definitionSnapshotReconciler interface {
	ReconcileSnapshot(context.Context, schedulersdk.DefinitionSnapshot) error
}

type definitionSnapshotHydrator interface {
	HydrateDefinitions(context.Context) (bool, error)
}

type definitionRepositoryOwner interface {
	DefinitionRepository() schedulerpersistence.DefinitionRepository
}

type databaseApplicationHost struct {
	service    DatabaseServiceOptions
	dialect    modulehost.Dialect
	downstream DownstreamHost
}

func NewDatabaseService(options DatabaseServiceOptions) (*DatabaseService, error) {
	if options.Database == nil || strings.TrimSpace(options.Driver) == "" || strings.TrimSpace(options.WorkerID) == "" || options.Downstreams == nil {
		return nil, fmt.Errorf("Scheduler SaaS database service is incomplete")
	}
	dialect, err := schedulerstore.Renderer(options.Driver, options.Schema)
	if err != nil {
		return nil, fmt.Errorf("Scheduler SaaS database dialect: %w", err)
	}
	parent := options.Context
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	service := &DatabaseService{options: options, dialect: dialect, ctx: ctx, cancel: cancel, applications: map[string]*databaseApplication{}, configured: map[string]bool{}}
	for _, application := range options.Applications {
		if err := application.Validate(); err != nil {
			cancel()
			return nil, err
		}
		key := strings.TrimSpace(application.RuntimeID)
		if service.configured[key] {
			cancel()
			return nil, fmt.Errorf("Scheduler SaaS application %q is configured more than once", key)
		}
		service.configured[key] = true
	}
	for _, application := range options.Applications {
		if _, err := service.application(ctx, application); err != nil {
			_ = service.Shutdown(context.Background())
			return nil, fmt.Errorf("restore Scheduler SaaS application %q: %w", application.RuntimeID, err)
		}
	}
	return service, nil
}

func (s *DatabaseService) application(ctx context.Context, ref schedulersdk.ApplicationRef) (*databaseApplication, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	key := strings.TrimSpace(ref.RuntimeID)
	if len(s.configured) != 0 && !s.configured[key] {
		return nil, fmt.Errorf("Scheduler SaaS application %q is not configured", key)
	}
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
	host := &databaseApplicationHost{service: s.options, dialect: s.dialect, downstream: downstream}
	state.host = host
	binding, err := moduleassembly.OpenSaaS(s.ctx, ref, host)
	if err != nil {
		return nil, err
	}
	state.binding = binding
	hydrator, ok := binding.(definitionSnapshotHydrator)
	if !ok {
		_ = binding.Close(ctx)
		return nil, fmt.Errorf("Scheduler SaaS binding cannot hydrate durable definitions")
	}
	hydrated, err := hydrator.HydrateDefinitions(ctx)
	if err != nil {
		_ = binding.Close(ctx)
		return nil, err
	}
	if hydrated {
		state.mu.Lock()
		state.ready = true
		state.mu.Unlock()
		state.startWorker(s.ctx, s.options.Worker)
	}
	s.applications[key] = state
	return state, nil
}

func (s *DatabaseService) Descriptor(ctx context.Context, ref schedulersdk.ApplicationRef) (schedulersdk.Descriptor, error) {
	if _, err := s.application(ctx, ref); err != nil {
		return schedulersdk.Descriptor{}, err
	}
	return schedulersdk.Descriptor{ProtocolVersion: schedulersdk.ProtocolVersionV1, Mode: schedulersdk.DeploymentModeSaaS, Capabilities: []string{"configuration_reconcile", "schedule_preview", "durable_trigger", "manual_trigger", "run_history", schedulersdk.CapabilityDefinitionPublicationFencing, schedulersdk.CapabilityScheduledPlanRecords, schedulersdk.CapabilityScheduledPlanDeletionRead}}, nil
}

func (s *DatabaseService) CreateScheduledPlan(ctx context.Context, ref schedulersdk.ApplicationRef, input schedulersdk.ScheduledPlanCreate) (schedulersdk.ScheduledPlanReceipt, error) {
	app, err := s.application(ctx, ref)
	if err != nil {
		return schedulersdk.ScheduledPlanReceipt{}, err
	}
	plans, ok := app.binding.(schedulersdk.ScheduledPlanService)
	if !ok {
		return schedulersdk.ScheduledPlanReceipt{}, fmt.Errorf("Scheduler plan service is unavailable")
	}
	receipt, err := plans.CreateScheduledPlan(ctx, input)
	if err != nil {
		return schedulersdk.ScheduledPlanReceipt{}, err
	}
	app.mu.Lock()
	app.ready = true
	app.mu.Unlock()
	app.startWorker(s.ctx, s.options.Worker)
	return receipt, nil
}

func (s *DatabaseService) GetScheduledPlan(ctx context.Context, ref schedulersdk.ApplicationRef, lookup schedulersdk.ScheduledPlanLookup) (schedulersdk.ScheduledPlan, error) {
	binding, err := s.binding(ctx, ref)
	if err != nil {
		return schedulersdk.ScheduledPlan{}, err
	}
	plans, ok := binding.(schedulersdk.ScheduledPlanService)
	if !ok {
		return schedulersdk.ScheduledPlan{}, fmt.Errorf("Scheduler plan service is unavailable")
	}
	return plans.GetScheduledPlan(ctx, lookup)
}

func (s *DatabaseService) scheduledPlans(ctx context.Context, ref schedulersdk.ApplicationRef) (schedulersdk.ScheduledPlanService, error) {
	binding, err := s.binding(ctx, ref)
	if err != nil {
		return nil, err
	}
	plans, ok := binding.(schedulersdk.ScheduledPlanService)
	if !ok {
		return nil, fmt.Errorf("Scheduler plan service is unavailable")
	}
	return plans, nil
}

func (s *DatabaseService) ListScheduledPlans(ctx context.Context, ref schedulersdk.ApplicationRef, input schedulersdk.ScheduledPlanList) (schedulersdk.ScheduledPlanPage, error) {
	plans, err := s.scheduledPlans(ctx, ref)
	if err != nil {
		return schedulersdk.ScheduledPlanPage{}, err
	}
	return plans.ListScheduledPlans(ctx, input)
}

func (s *DatabaseService) UpdateScheduledPlan(ctx context.Context, ref schedulersdk.ApplicationRef, input schedulersdk.ScheduledPlanUpdate) (schedulersdk.ScheduledPlanReceipt, error) {
	plans, err := s.scheduledPlans(ctx, ref)
	if err != nil {
		return schedulersdk.ScheduledPlanReceipt{}, err
	}
	return plans.UpdateScheduledPlan(ctx, input)
}

func (s *DatabaseService) PauseScheduledPlan(ctx context.Context, ref schedulersdk.ApplicationRef, input schedulersdk.ScheduledPlanStatusChange) (schedulersdk.ScheduledPlanReceipt, error) {
	plans, err := s.scheduledPlans(ctx, ref)
	if err != nil {
		return schedulersdk.ScheduledPlanReceipt{}, err
	}
	return plans.PauseScheduledPlan(ctx, input)
}

func (s *DatabaseService) ResumeScheduledPlan(ctx context.Context, ref schedulersdk.ApplicationRef, input schedulersdk.ScheduledPlanStatusChange) (schedulersdk.ScheduledPlanReceipt, error) {
	plans, err := s.scheduledPlans(ctx, ref)
	if err != nil {
		return schedulersdk.ScheduledPlanReceipt{}, err
	}
	return plans.ResumeScheduledPlan(ctx, input)
}

func (s *DatabaseService) DeleteScheduledPlan(ctx context.Context, ref schedulersdk.ApplicationRef, input schedulersdk.ScheduledPlanStatusChange) (schedulersdk.ScheduledPlanDeleteReceipt, error) {
	plans, err := s.scheduledPlans(ctx, ref)
	if err != nil {
		return schedulersdk.ScheduledPlanDeleteReceipt{}, err
	}
	return plans.DeleteScheduledPlan(ctx, input)
}

func (s *DatabaseService) BeginDefinitionPublisherSession(ctx context.Context, ref schedulersdk.ApplicationRef) (schedulersdk.DefinitionPublisherSession, error) {
	app, err := s.application(ctx, ref)
	if err != nil {
		return schedulersdk.DefinitionPublisherSession{}, err
	}
	app.reconcileMu.Lock()
	defer app.reconcileMu.Unlock()
	owner, ok := app.binding.(definitionRepositoryOwner)
	if !ok {
		return schedulersdk.DefinitionPublisherSession{}, fmt.Errorf("Scheduler SaaS binding has no definition repository")
	}
	repository, ok := owner.DefinitionRepository().(schedulerpersistence.DefinitionPublicationRepository)
	if !ok {
		return schedulersdk.DefinitionPublisherSession{}, schedulersdk.ErrDefinitionPublicationCapabilityRequired
	}
	return repository.BeginDefinitionPublisherSession(ctx)
}

func (s *DatabaseService) Reconcile(ctx context.Context, ref schedulersdk.ApplicationRef, snapshot schedulersdk.DefinitionSnapshot) error {
	app, err := s.application(ctx, ref)
	if err != nil {
		return err
	}
	app.reconcileMu.Lock()
	defer app.reconcileMu.Unlock()
	reconciler, ok := app.binding.(definitionSnapshotReconciler)
	if !ok {
		return fmt.Errorf("Scheduler SaaS binding cannot reconcile an immutable Runtime snapshot")
	}
	if err := reconciler.ReconcileSnapshot(ctx, snapshot); err != nil {
		return err
	}
	app.mu.Lock()
	app.ready = true
	app.mu.Unlock()
	app.startWorker(s.ctx, s.options.Worker)
	return nil
}

// startWorker is called after a real Runtime snapshot or a validated scheduled
// plan has been persisted. A read that lazily opens an application after a
// process restart must never publish an empty snapshot or disable durable
// definitions.
func (a *databaseApplication) startWorker(parent context.Context, worker schedulersdk.WorkerConfig) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cancel != nil {
		return
	}
	if worker == (schedulersdk.WorkerConfig{}) {
		worker.Enabled = true
	}
	workerCtx, cancel := context.WithCancel(parent)
	a.cancel = cancel
	a.done = a.binding.Start(workerCtx, worker)
}

func (a *databaseApplication) requireReady() error {
	a.mu.RLock()
	ready := a.ready
	a.mu.RUnlock()
	if !ready {
		return fmt.Errorf("Scheduler SaaS application requires a current Runtime definition snapshot")
	}
	return nil
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
	if err := app.requireReady(); err != nil {
		return 0, err
	}
	return app.binding.Tick(ctx, now, limit)
}
func (s *DatabaseService) TriggerNow(ctx context.Context, ref schedulersdk.ApplicationRef, key, reason string) (schedulersdk.Run, error) {
	app, err := s.application(ctx, ref)
	if err != nil {
		return schedulersdk.Run{}, err
	}
	if err := app.requireReady(); err != nil {
		return schedulersdk.Run{}, err
	}
	return app.binding.TriggerNow(ctx, key, reason)
}
func (s *DatabaseService) Reschedule(ctx context.Context, ref schedulersdk.ApplicationRef, key string, nextRunAt time.Time, reason string) error {
	app, err := s.application(ctx, ref)
	if err != nil {
		return err
	}
	if err := app.requireReady(); err != nil {
		return err
	}
	return app.binding.Reschedule(ctx, key, nextRunAt, reason)
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
	app, err := s.application(ctx, ref)
	if err != nil {
		return schedulersdk.Run{}, err
	}
	if err := app.requireReady(); err != nil {
		return schedulersdk.Run{}, err
	}
	return app.binding.RetryRun(ctx, id, reason)
}
func (s *DatabaseService) CancelRun(ctx context.Context, ref schedulersdk.ApplicationRef, id, reason string) (schedulersdk.Run, error) {
	app, err := s.application(ctx, ref)
	if err != nil {
		return schedulersdk.Run{}, err
	}
	if err := app.requireReady(); err != nil {
		return schedulersdk.Run{}, err
	}
	return app.binding.CancelRun(ctx, id, reason)
}
func (s *DatabaseService) DeadLetters(ctx context.Context, ref schedulersdk.ApplicationRef, limit int) ([]schedulersdk.DeadLetter, error) {
	binding, err := s.binding(ctx, ref)
	if err != nil {
		return nil, err
	}
	return binding.DeadLetters(ctx, limit)
}
func (s *DatabaseService) DeadLetter(ctx context.Context, ref schedulersdk.ApplicationRef, id string) (schedulersdk.DeadLetter, error) {
	binding, err := s.binding(ctx, ref)
	if err != nil {
		return schedulersdk.DeadLetter{}, err
	}
	return binding.DeadLetter(ctx, id)
}
func (s *DatabaseService) ResolveDeadLetter(ctx context.Context, ref schedulersdk.ApplicationRef, id, reason string) (schedulersdk.DeadLetter, error) {
	app, err := s.application(ctx, ref)
	if err != nil {
		return schedulersdk.DeadLetter{}, err
	}
	if err := app.requireReady(); err != nil {
		return schedulersdk.DeadLetter{}, err
	}
	return app.binding.ResolveDeadLetter(ctx, id, reason)
}
func (s *DatabaseService) RequeueDeadLetter(ctx context.Context, ref schedulersdk.ApplicationRef, id, reason string) (schedulersdk.Run, error) {
	app, err := s.application(ctx, ref)
	if err != nil {
		return schedulersdk.Run{}, err
	}
	if err := app.requireReady(); err != nil {
		return schedulersdk.Run{}, err
	}
	return app.binding.RequeueDeadLetter(ctx, id, reason)
}
func (s *DatabaseService) Close(ctx context.Context, ref schedulersdk.ApplicationRef) error {
	// Application lifecycle belongs to the Scheduler process. Legacy remote
	// close requests are intentionally local no-ops and cannot stop a worker.
	return nil
}

// Shutdown stops every application worker owned by this SaaS process.
func (s *DatabaseService) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.cancel()
	s.mu.Lock()
	applications := s.applications
	s.applications = map[string]*databaseApplication{}
	s.mu.Unlock()
	var firstErr error
	for _, app := range applications {
		if app.cancel != nil {
			app.cancel()
		}
		if err := app.binding.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (h *databaseApplicationHost) Definitions() modulehost.DefinitionProvider { return h }
func (h *databaseApplicationHost) Dispatcher() modulehost.Dispatcher          { return h.downstream }
func (h *databaseApplicationHost) HTTPConnections() modulehost.HTTPConnectionProvider {
	return h.downstream
}
func (h *databaseApplicationHost) Database() modulehost.Database { return h.service.Database }
func (h *databaseApplicationHost) Dialect() modulehost.Dialect   { return h.dialect }
func (h *databaseApplicationHost) Migrations() modulehost.MigrationRegistrar {
	return schedulerSaaSMigrations{host: h}
}
func (h *databaseApplicationHost) Driver() string   { return h.service.Driver }
func (h *databaseApplicationHost) Schema() string   { return h.service.Schema }
func (h *databaseApplicationHost) WorkerID() string { return h.service.WorkerID }
func (h *databaseApplicationHost) Snapshot(context.Context) (schedulersdk.DefinitionSnapshot, error) {
	return schedulersdk.DefinitionSnapshot{}, fmt.Errorf("Scheduler SaaS definitions arrive through the fenced publication protocol")
}

var _ moduleassembly.SaaSHost = (*databaseApplicationHost)(nil)

type schedulerSaaSMigrations struct{ host *databaseApplicationHost }

func (m schedulerSaaSMigrations) Driver() string { return m.host.service.Driver }
func (m schedulerSaaSMigrations) Schema() string { return m.host.service.Schema }
func (m schedulerSaaSMigrations) ApplyOwnedMigrations(ctx context.Context, owner string, migrations []modulehost.SchemaMigration) error {
	return schedulermigration.EnsureSchema(ctx, m.host.service.Database, m.host.dialect, owner, migrations)
}
