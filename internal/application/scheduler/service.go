package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	actioncontract "github.com/domainry/domainry-foundation/action"
	"github.com/domainry/domainry-foundation/modulehttp"
	"github.com/domainry/domainry-foundation/worker"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulerpersistence "github.com/domainry/domainry-scheduler-sdk/persistence"
	"github.com/domainry/domainry-scheduler-sdk/schedule"
	domainservice "github.com/domainry/domainry-scheduler/internal/domain/scheduler/service"
)

type Service struct {
	application          schedulersdk.ApplicationRef
	host                 modulehost.Host
	directHTTP           modulehost.Dispatcher
	runs                 modulehost.RunStore
	definitionRepository schedulerpersistence.DefinitionRepository
	planRepository       schedulerpersistence.ScheduledPlanRepository
	planRecovery         schedulerpersistence.ScheduledPlanRecoveryRepository
	mode                 schedulersdk.DeploymentMode
	ctx                  context.Context
	cancel               context.CancelFunc
	mu                   sync.RWMutex
	reconcileMu          sync.Mutex
	reconcileGeneration  atomic.Uint64
	definitions          map[string]schedulersdk.Definition
	leaseTTL             time.Duration
	dispatchTimeout      time.Duration
	lifecycleMu          sync.Mutex
	workerDone           chan struct{}
	closed               bool
	now                  func() time.Time
	httpAdapters         []modulehttp.Adapter
}

type fencedDefinitionSnapshotProjector interface {
	ApplyDefinitionSnapshot(context.Context, schedulerpersistence.DefinitionSnapshot, time.Time) (bool, error)
}

func NewService(ctx context.Context, cancel context.CancelFunc, application schedulersdk.ApplicationRef, host modulehost.Host, directHTTP modulehost.Dispatcher, runs modulehost.RunStore, definitions schedulerpersistence.DefinitionRepository, mode schedulersdk.DeploymentMode) *Service {
	if ctx == nil {
		ctx = context.Background()
	}
	if cancel == nil {
		ctx, cancel = context.WithCancel(ctx)
	}
	defaults := schedulersdk.NormalizeWorkerConfig(schedulersdk.WorkerConfig{})
	if backlog, ok := runs.(modulehost.TriggerBacklogStore); ok {
		_ = backlog.ConfigureTriggerBacklogLimit(defaults.MaxPendingTriggers)
	}
	return &Service{application: application, host: host, directHTTP: directHTTP, runs: runs, definitionRepository: definitions, mode: mode, ctx: ctx, cancel: cancel, definitions: map[string]schedulersdk.Definition{}, leaseTTL: defaults.LeaseTTL, dispatchTimeout: defaults.DispatchTimeout, now: time.Now}
}

func (b *Service) Descriptor() schedulersdk.Descriptor {
	capabilities := []string{"configuration_reconcile", "schedule_preview", "durable_trigger", "manual_trigger", "run_evidence", schedulersdk.CapabilityScheduledPlanRecords, schedulersdk.CapabilityScheduledPlanDeletionRead, schedulersdk.CapabilityTriggerBacklog}
	if b.mode == schedulersdk.DeploymentModeSaaS {
		capabilities = append(capabilities, schedulersdk.CapabilityDefinitionPublicationFencing)
	}
	return schedulersdk.Descriptor{ProtocolVersion: schedulersdk.ProtocolVersionV1, Mode: b.mode, Capabilities: capabilities}
}

func (*Service) AuthorizationActions() ([]actioncontract.ActionDefinition, error) {
	return schedulersdk.SchedulerAuthorizationActions()
}

func (b *Service) SetHTTPAdapters(adapters []modulehttp.Adapter) {
	b.httpAdapters = append([]modulehttp.Adapter(nil), adapters...)
}

func (b *Service) HTTPAdapters() []modulehttp.Adapter {
	return append([]modulehttp.Adapter(nil), b.httpAdapters...)
}

func (b *Service) Reconcile(ctx context.Context) error {
	generation := b.reconcileGeneration.Add(1)
	b.reconcileMu.Lock()
	defer b.reconcileMu.Unlock()
	if generation != b.reconcileGeneration.Load() {
		return nil
	}
	snapshot, err := b.host.Definitions().Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("read Scheduler definitions: %w", err)
	}
	return b.reconcileSnapshot(ctx, snapshot)
}

// ReconcileSnapshot lets the SaaS protocol synchronize the immutable request
// snapshot directly instead of publishing it through a mutable host slot. The
// generation is claimed before waiting on the serialization lock, so an older
// invocation can never run after a newer one has begun.
func (b *Service) ReconcileSnapshot(ctx context.Context, snapshot schedulersdk.DefinitionSnapshot) error {
	b.reconcileMu.Lock()
	defer b.reconcileMu.Unlock()
	return b.reconcileSnapshot(ctx, snapshot)
}

// HydrateDefinitions restores one previously published, application-scoped
// snapshot without consulting the mutable host provider or writing definition
// rows. The boolean is false only when this application has never published a
// durable snapshot, including the valid case of a published empty snapshot.
func (b *Service) HydrateDefinitions(ctx context.Context) (bool, error) {
	generation := b.reconcileGeneration.Add(1)
	b.reconcileMu.Lock()
	defer b.reconcileMu.Unlock()
	if generation != b.reconcileGeneration.Load() {
		return false, nil
	}
	definitions, err := b.convergePersistedDefinitions(ctx)
	if err != nil {
		return false, err
	}
	plans, err := b.hydrateScheduledPlans(ctx)
	return definitions || plans, err
}

func (b *Service) reconcileSnapshot(ctx context.Context, snapshot schedulersdk.DefinitionSnapshot) error {
	if b.definitionRepository == nil {
		return fmt.Errorf("Scheduler definition repository is unavailable")
	}
	for _, definition := range snapshot.Definitions {
		definition = definition.Normalize()
		if strings.HasPrefix(definition.Key, scheduledPlanDefinitionPrefix) {
			return fmt.Errorf("scheduler definition key %q uses the reserved plan namespace", definition.Key)
		}
		if err := definition.Validate(); err != nil {
			return err
		}
		if err := schedule.Validate(definition.Schedule); err != nil {
			return fmt.Errorf("scheduler definition %s: %w", definition.Key, err)
		}
	}
	persisted := schedulerpersistence.DefinitionSnapshot{
		Revision: snapshot.Revision, SchemaVersion: fmt.Sprint(snapshot.Revision), SourceKind: "runtime_host", SourceID: b.application.RuntimeID, Definitions: snapshot.Definitions,
	}
	switch b.mode {
	case schedulersdk.DeploymentModeModule:
		if err := schedulersdk.ValidateModuleDefinitionSnapshot(snapshot); err != nil {
			return err
		}
	case schedulersdk.DeploymentModeSaaS:
		if snapshot.PublisherSession == nil {
			return schedulersdk.ErrDefinitionPublicationRequired
		}
		fence, err := snapshot.PublisherSession.PublisherFence()
		if err != nil {
			return err
		}
		contentHash, err := schedulersdk.DefinitionSnapshotContentSHA256(snapshot.Definitions)
		if err != nil {
			return err
		}
		persisted.PublisherFence = &fence
		persisted.ContentSHA256 = contentHash
	default:
		return fmt.Errorf("Scheduler deployment mode %q is unsupported", b.mode)
	}
	if err := b.definitionRepository.SyncDefinitions(ctx, persisted); err != nil {
		return fmt.Errorf("persist Scheduler definitions: %w", err)
	}
	found, err := b.convergePersistedDefinitions(ctx)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("Scheduler persisted definition snapshot is unavailable after reconcile")
	}
	return nil
}

func (b *Service) convergePersistedDefinitions(ctx context.Context) (bool, error) {
	if b.definitionRepository == nil {
		return false, fmt.Errorf("Scheduler definition repository is unavailable")
	}
	for attempt := 0; attempt < 64; attempt++ {
		persisted, err := b.definitionRepository.DefinitionSnapshot(ctx)
		if err != nil {
			return false, fmt.Errorf("load Scheduler definitions: %w", err)
		}
		if strings.TrimSpace(persisted.SchemaVersion) == "" {
			return false, nil
		}
		applied, err := b.applyPersistedDefinitions(ctx, persisted)
		if err != nil {
			return false, err
		}
		if applied {
			return true, nil
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
	}
	return false, fmt.Errorf("Scheduler definition projection did not converge on the canonical snapshot")
}

func (b *Service) applyPersistedDefinitions(ctx context.Context, persisted schedulerpersistence.DefinitionSnapshot) (bool, error) {
	now := b.now().UTC()
	next := make(map[string]schedulersdk.Definition, len(persisted.Definitions))
	active := make([]string, 0, len(persisted.Definitions))
	for _, definition := range persisted.Definitions {
		definition = definition.Normalize()
		if strings.HasPrefix(definition.Key, scheduledPlanDefinitionPrefix) {
			return false, fmt.Errorf("scheduler definition key %q uses the reserved plan namespace", definition.Key)
		}
		if err := definition.Validate(); err != nil {
			return false, err
		}
		if err := schedule.Validate(definition.Schedule); err != nil {
			return false, fmt.Errorf("scheduler definition %s: %w", definition.Key, err)
		}
		next[definition.Key] = definition
		if persisted.PublisherFence == nil && strings.EqualFold(strings.TrimSpace(definition.Status), "enabled") {
			active = append(active, definition.Key)
			nextRunAt := schedule.NextSchedule(definition.Schedule, now)
			if !definition.InitialNextRunAt.IsZero() && definition.InitialNextRunAt.Before(nextRunAt) {
				nextRunAt = definition.InitialNextRunAt.UTC()
			}
			if err := b.runs.Reconcile(ctx, definition, nextRunAt); err != nil {
				return false, fmt.Errorf("reconcile Scheduler definition %s: %w", definition.Key, err)
			}
		}
	}
	if persisted.PublisherFence != nil {
		projector, ok := b.runs.(fencedDefinitionSnapshotProjector)
		if !ok {
			return false, fmt.Errorf("Scheduler SaaS run store cannot apply a fenced definition snapshot")
		}
		applied, err := projector.ApplyDefinitionSnapshot(ctx, persisted, now)
		if err != nil {
			return false, fmt.Errorf("apply fenced Scheduler definition projection: %w", err)
		}
		if !applied {
			return false, nil
		}
	} else {
		sort.Strings(active)
		if err := b.runs.DisableMissing(ctx, active, persisted.Revision); err != nil {
			return false, fmt.Errorf("disable removed Scheduler definitions: %w", err)
		}
	}
	b.mu.Lock()
	b.definitions = next
	b.mu.Unlock()
	return true, nil
}

func (b *Service) DefinitionRepository() schedulerpersistence.DefinitionRepository {
	return b.definitionRepository
}

var _ schedulerpersistence.Binding = (*Service)(nil)

func (*Service) Preview(ctx context.Context, value schedulersdk.Schedule, after time.Time, count int) ([]time.Time, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if count <= 0 {
		count = 5
	}
	if count > 100 {
		count = 100
	}
	if after.IsZero() {
		after = time.Now().UTC()
	}
	return schedule.PreviewSchedule(ctx, value, after, count)
}

func (b *Service) Tick(ctx context.Context, now time.Time, limit int) (int, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if limit <= 0 {
		limit = 25
	}
	due, err := b.runs.Due(ctx, now, limit)
	if err != nil {
		return 0, err
	}
	processed := 0
	var firstErr error
	for _, item := range due {
		if err := b.dispatch(ctx, item); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		processed++
	}
	return processed, firstErr
}

func (b *Service) TriggerNow(ctx context.Context, key, reason string) (schedulersdk.Run, error) {
	key = strings.TrimSpace(key)
	definition, found, err := b.publishedDefinition(ctx, key)
	if err != nil {
		return schedulersdk.Run{}, err
	}
	if !found {
		return schedulersdk.Run{}, fmt.Errorf("scheduler definition %q is not published", key)
	}
	now := time.Now().UTC()
	metadata, _ := json.Marshal(map[string]any{"trigger": "manual", "reason": strings.TrimSpace(reason)})
	item := modulehost.DueTrigger{Definition: definition, ScheduledFor: now, Metadata: metadata}
	run, claimed, err := b.runs.Claim(ctx, item, b.currentLeaseTTL())
	if err != nil || !claimed {
		return run, err
	}
	return run, b.dispatchClaimed(ctx, run, definition)
}

func (b *Service) publishedDefinition(ctx context.Context, key string) (schedulersdk.Definition, bool, error) {
	if b.mode == schedulersdk.DeploymentModeSaaS {
		if b.definitionRepository == nil {
			return schedulersdk.Definition{}, false, fmt.Errorf("Scheduler definition repository is unavailable")
		}
		snapshot, err := b.definitionRepository.DefinitionSnapshot(ctx)
		if err != nil {
			return schedulersdk.Definition{}, false, fmt.Errorf("load Scheduler definitions: %w", err)
		}
		for _, definition := range snapshot.Definitions {
			if strings.TrimSpace(definition.Key) == key {
				return definition.Normalize(), true, nil
			}
		}
		return schedulersdk.Definition{}, false, nil
	}
	b.mu.RLock()
	definition, found := b.definitions[key]
	b.mu.RUnlock()
	return definition, found, nil
}

func (b *Service) Reschedule(ctx context.Context, key string, nextRunAt time.Time, reason string) error {
	return b.runs.Reschedule(ctx, strings.TrimSpace(key), nextRunAt.UTC(), strings.TrimSpace(reason))
}

func (b *Service) dispatch(ctx context.Context, item modulehost.DueTrigger) error {
	run, claimed, err := b.runs.Claim(ctx, item, b.currentLeaseTTL())
	if err != nil || !claimed {
		return err
	}
	return b.dispatchClaimed(ctx, run, item.Definition)
}

func (b *Service) dispatchClaimed(ctx context.Context, run schedulersdk.Run, definition schedulersdk.Definition) error {
	timeout := b.currentDispatchTimeout()
	if definition.Policy.Timeout > 0 && definition.Policy.Timeout < timeout {
		timeout = definition.Policy.Timeout
	}
	dispatchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	workCtx := dispatchCtx
	stopHeartbeat := func() error { return nil }
	if run.Lease.Valid() {
		ttl := b.currentLeaseTTL()
		interval := ttl / 3
		workCtx, stopHeartbeat = worker.WithHeartbeat(dispatchCtx, interval, func(heartbeatCtx context.Context) error {
			_, owned, err := b.runs.Renew(heartbeatCtx, run, ttl)
			if err != nil {
				return err
			}
			if !owned {
				return fmt.Errorf("Scheduler run %q lease lost", run.Trigger.RunID)
			}
			return nil
		})
	}
	var receipt schedulersdk.DownstreamReceipt
	var err error
	if run.Trigger.Target.Type == "http" && strings.EqualFold(strings.TrimSpace(run.Trigger.Target.DispatchMode), "direct") {
		receipt, err = b.directHTTP.Dispatch(workCtx, run.Trigger)
	} else {
		receipt, err = b.host.Dispatcher().Dispatch(workCtx, run.Trigger)
	}
	leaseErr := stopHeartbeat()
	if leaseErr != nil {
		return leaseErr
	}
	if err != nil {
		_ = b.runs.Fail(ctx, run, err, domainservice.NextRetry(definition.Policy, run.Trigger.Attempt, time.Now().UTC()))
		return err
	}
	if strings.TrimSpace(receipt.ID) == "" {
		err = fmt.Errorf("downstream owner returned no durable receipt")
		_ = b.runs.Fail(ctx, run, err, domainservice.NextRetry(definition.Policy, run.Trigger.Attempt, time.Now().UTC()))
		return err
	}
	return b.runs.Accept(ctx, run, receipt)
}

func (b *Service) currentLeaseTTL() time.Duration {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.leaseTTL
}

func (b *Service) currentDispatchTimeout() time.Duration {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.dispatchTimeout
}

func (b *Service) TriggerBacklog(ctx context.Context) (schedulersdk.TriggerBacklog, error) {
	backlog, ok := b.runs.(modulehost.TriggerBacklogStore)
	if !ok {
		return schedulersdk.TriggerBacklog{}, fmt.Errorf("Scheduler trigger backlog is unavailable")
	}
	return backlog.TriggerBacklog(ctx)
}

func (b *Service) Runs(ctx context.Context, limit int) ([]schedulersdk.Run, error) {
	return b.runs.List(ctx, limit)
}
func (b *Service) Run(ctx context.Context, id string) (schedulersdk.Run, error) {
	return b.runs.Get(ctx, id)
}
func (b *Service) RetryRun(ctx context.Context, id, reason string) (schedulersdk.Run, error) {
	return b.runs.Retry(ctx, id, reason)
}
func (b *Service) CancelRun(ctx context.Context, id, reason string) (schedulersdk.Run, error) {
	return b.runs.Cancel(ctx, id, reason)
}
func (b *Service) DeadLetters(ctx context.Context, limit int) ([]schedulersdk.DeadLetter, error) {
	return b.runs.DeadLetters(ctx, limit)
}
func (b *Service) DeadLetter(ctx context.Context, id string) (schedulersdk.DeadLetter, error) {
	return b.runs.DeadLetter(ctx, id)
}
func (b *Service) ResolveDeadLetter(ctx context.Context, id, reason string) (schedulersdk.DeadLetter, error) {
	return b.runs.ResolveDeadLetter(ctx, id, reason)
}
func (b *Service) RequeueDeadLetter(ctx context.Context, id, reason string) (schedulersdk.Run, error) {
	return b.runs.RequeueDeadLetter(ctx, id, reason)
}

func (b *Service) Start(ctx context.Context, config schedulersdk.WorkerConfig) <-chan struct{} {
	config = schedulersdk.NormalizeWorkerConfig(config)
	if !config.Enabled {
		return closedLifecycleDone()
	}
	if ctx == nil || ctx.Err() != nil {
		return closedLifecycleDone()
	}
	// One Start call owns each live worker generation. Concurrent calls join
	// that generation and receive the same completion signal; after its caller
	// context is canceled and done closes, a later Start creates a new one.
	b.lifecycleMu.Lock()
	if b.closed || b.ctx.Err() != nil {
		b.lifecycleMu.Unlock()
		return closedLifecycleDone()
	}
	if b.workerDone != nil {
		done := b.workerDone
		b.lifecycleMu.Unlock()
		return done
	}
	if backlog, ok := b.runs.(modulehost.TriggerBacklogStore); ok {
		if err := backlog.ConfigureTriggerBacklogLimit(config.MaxPendingTriggers); err != nil {
			b.lifecycleMu.Unlock()
			return closedLifecycleDone()
		}
	}
	b.mu.Lock()
	b.leaseTTL = config.LeaseTTL
	b.dispatchTimeout = config.DispatchTimeout
	b.mu.Unlock()
	done := make(chan struct{})
	runCtx, cancel := context.WithCancel(ctx)
	stopOwnerCancellation := context.AfterFunc(b.ctx, cancel)
	b.workerDone = done
	b.lifecycleMu.Unlock()

	go b.runWorker(runCtx, cancel, stopOwnerCancellation, config, done)
	return done
}

var _ schedulersdk.TriggerBacklogProvider = (*Service)(nil)

func (b *Service) runWorker(ctx context.Context, cancel context.CancelFunc, stopOwnerCancellation func() bool, config schedulersdk.WorkerConfig, done chan struct{}) {
	defer func() {
		stopOwnerCancellation()
		cancel()
		b.lifecycleMu.Lock()
		if b.workerDone == done {
			b.workerDone = nil
		}
		close(done)
		b.lifecycleMu.Unlock()
	}()
	// Module hosts own an in-process definition provider that is ready at
	// worker start. SaaS snapshots arrive through the private Reconcile
	// protocol and are synchronized before its worker is started; treating
	// the initial empty SaaS host as a publication can disable durable state.
	if b.mode == schedulersdk.DeploymentModeModule {
		_ = b.Reconcile(ctx)
		_, _ = b.hydrateScheduledPlans(ctx)
	}
	loopDone := worker.StartNamedLoop(ctx, "scheduler", config.PollInterval, func() {
		_, _ = b.Tick(ctx, time.Now().UTC(), config.BatchSize)
	})
	<-loopDone
}

func (b *Service) Close(ctx context.Context) error {
	b.lifecycleMu.Lock()
	shouldCancel := !b.closed
	b.closed = true
	done := b.workerDone
	b.lifecycleMu.Unlock()
	if shouldCancel {
		b.cancel()
	}
	if done == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func closedLifecycleDone() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

var _ schedulersdk.Binding = (*Service)(nil)
var _ actioncontract.Provider = (*Service)(nil)
var _ modulehttp.Provider = (*Service)(nil)
