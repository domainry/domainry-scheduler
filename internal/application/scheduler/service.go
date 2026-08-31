package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/domainry/domainry-foundation/worker"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulerrepository "github.com/domainry/domainry-scheduler-sdk/repository"
	"github.com/domainry/domainry-scheduler-sdk/schedule"
	domainservice "github.com/domainry/domainry-scheduler/internal/domain/scheduler/service"
)

type Service struct {
	application          schedulersdk.ApplicationRef
	host                 modulehost.Host
	directHTTP           modulehost.Dispatcher
	runs                 modulehost.RunStore
	definitionRepository schedulerrepository.DefinitionRepository
	mode                 schedulersdk.DeploymentMode
	ctx                  context.Context
	cancel               context.CancelFunc
	mu                   sync.RWMutex
	definitions          map[string]schedulersdk.Definition
	leaseTTL             time.Duration
	startOnce            sync.Once
	closeOnce            sync.Once
}

func NewService(ctx context.Context, cancel context.CancelFunc, application schedulersdk.ApplicationRef, host modulehost.Host, directHTTP modulehost.Dispatcher, runs modulehost.RunStore, definitions schedulerrepository.DefinitionRepository, mode schedulersdk.DeploymentMode) *Service {
	return &Service{application: application, host: host, directHTTP: directHTTP, runs: runs, definitionRepository: definitions, mode: mode, ctx: ctx, cancel: cancel, definitions: map[string]schedulersdk.Definition{}, leaseTTL: 5 * time.Minute}
}

func (b *Service) Descriptor() schedulersdk.Descriptor {
	return schedulersdk.Descriptor{ProtocolVersion: schedulersdk.ProtocolVersionV1, Mode: b.mode, Capabilities: []string{"configuration_reconcile", "schedule_preview", "durable_trigger", "manual_trigger", "run_evidence"}}
}

func (b *Service) Reconcile(ctx context.Context) error {
	snapshot, err := b.host.Definitions().Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("read Scheduler definitions: %w", err)
	}
	if b.definitionRepository == nil {
		return fmt.Errorf("Scheduler definition repository is unavailable")
	}
	if err := b.definitionRepository.SyncDefinitions(ctx, schedulerrepository.DefinitionSnapshot{
		Revision: snapshot.Revision, SchemaVersion: fmt.Sprint(snapshot.Revision), SourceKind: "runtime_host", SourceID: b.application.RuntimeID, Definitions: snapshot.Definitions,
	}); err != nil {
		return fmt.Errorf("persist Scheduler definitions: %w", err)
	}
	persisted, err := b.definitionRepository.DefinitionSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("load Scheduler definitions: %w", err)
	}
	persisted.Revision = snapshot.Revision
	now := time.Now().UTC()
	next := make(map[string]schedulersdk.Definition, len(persisted.Definitions))
	active := make([]string, 0, len(persisted.Definitions))
	for _, definition := range persisted.Definitions {
		definition = definition.Normalize()
		if err := definition.Validate(); err != nil {
			return err
		}
		if err := schedule.Validate(definition.Schedule); err != nil {
			return fmt.Errorf("scheduler definition %s: %w", definition.Key, err)
		}
		next[definition.Key] = definition
		if strings.EqualFold(strings.TrimSpace(definition.Status), "enabled") {
			active = append(active, definition.Key)
			next := schedule.NextSchedule(definition.Schedule, now)
			if !definition.InitialNextRunAt.IsZero() && definition.InitialNextRunAt.Before(next) {
				next = definition.InitialNextRunAt.UTC()
			}
			if err := b.runs.Reconcile(ctx, definition, next); err != nil {
				return fmt.Errorf("reconcile Scheduler definition %s: %w", definition.Key, err)
			}
		}
	}
	sort.Strings(active)
	if err := b.runs.DisableMissing(ctx, active, persisted.Revision); err != nil {
		return fmt.Errorf("disable removed Scheduler definitions: %w", err)
	}
	b.mu.Lock()
	b.definitions = next
	b.mu.Unlock()
	return nil
}

func (b *Service) DefinitionRepository() schedulerrepository.DefinitionRepository {
	return b.definitionRepository
}

var _ schedulerrepository.Binding = (*Service)(nil)

func (*Service) Preview(ctx context.Context, value schedulersdk.Schedule, after time.Time, count int) ([]time.Time, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := schedule.Validate(value); err != nil {
		return nil, err
	}
	if count <= 0 {
		count = 5
	}
	if count > 100 {
		count = 100
	}
	cursor := after
	if cursor.IsZero() {
		cursor = time.Now().UTC()
	}
	result := make([]time.Time, 0, count)
	for len(result) < count {
		cursor = schedule.NextSchedule(value, cursor)
		result = append(result, cursor)
	}
	return result, nil
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
	b.mu.RLock()
	definition, found := b.definitions[strings.TrimSpace(key)]
	b.mu.RUnlock()
	if !found {
		return schedulersdk.Run{}, fmt.Errorf("scheduler definition %q is not published", key)
	}
	now := time.Now().UTC()
	item := modulehost.DueTrigger{Definition: definition, ScheduledFor: now}
	run, claimed, err := b.runs.Claim(ctx, item, b.currentLeaseTTL())
	if err != nil || !claimed {
		return run, err
	}
	metadata, _ := json.Marshal(map[string]any{"trigger": "manual", "reason": strings.TrimSpace(reason)})
	run.Trigger.Metadata = metadata
	return run, b.dispatchClaimed(ctx, run, definition)
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
	dispatchCtx, cancelDispatch := context.WithCancel(ctx)
	cancel := func() { cancelDispatch() }
	if definition.Policy.Timeout > 0 {
		dispatchCtx, cancel = context.WithTimeout(dispatchCtx, definition.Policy.Timeout)
	}
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
	done := make(chan struct{})
	config = schedulersdk.NormalizeWorkerConfig(config)
	b.mu.Lock()
	b.leaseTTL = config.LeaseTTL
	b.mu.Unlock()
	if !config.Enabled {
		close(done)
		return done
	}
	started := false
	b.startOnce.Do(func() {
		started = true
		go func() {
			defer close(done)
			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			go func() {
				select {
				case <-b.ctx.Done():
					cancel()
				case <-runCtx.Done():
				}
			}()
			_ = b.Reconcile(runCtx)
			loopDone := worker.StartNamedLoop(runCtx, "scheduler", config.PollInterval, func() {
				_, _ = b.Tick(runCtx, time.Now().UTC(), config.BatchSize)
			})
			<-loopDone
		}()
	})
	if !started {
		close(done)
	}
	return done
}

func (b *Service) Close(context.Context) error { b.closeOnce.Do(b.cancel); return nil }

var _ schedulersdk.Binding = (*Service)(nil)
