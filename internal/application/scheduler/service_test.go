package scheduler

import (
	"context"
	"testing"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulerpersistence "github.com/domainry/domainry-scheduler-sdk/persistence"
)

type hostStub struct {
	definition schedulersdk.Definition
	due        []modulehost.DueTrigger
	dispatched int
	accepted   int
	lease      bool
	renewed    int
	loseLease  bool
	dispatchFn func(context.Context) (schedulersdk.DownstreamReceipt, error)
	snapshot   schedulerpersistence.DefinitionSnapshot
}

func (h *hostStub) SyncDefinitions(_ context.Context, snapshot schedulerpersistence.DefinitionSnapshot) error {
	h.snapshot = snapshot
	return nil
}
func (h *hostStub) DefinitionSnapshot(context.Context) (schedulerpersistence.DefinitionSnapshot, error) {
	return h.snapshot, nil
}

func (h *hostStub) Definitions() modulehost.DefinitionProvider         { return h }
func (h *hostStub) Runs() modulehost.RunStore                          { return h }
func (h *hostStub) Dispatcher() modulehost.Dispatcher                  { return h }
func (h *hostStub) HTTPConnections() modulehost.HTTPConnectionProvider { return h }
func (h *hostStub) Snapshot(context.Context) (schedulersdk.DefinitionSnapshot, error) {
	return schedulersdk.DefinitionSnapshot{Revision: 1, Definitions: []schedulersdk.Definition{h.definition}}, nil
}
func (*hostStub) Reconcile(context.Context, schedulersdk.Definition, time.Time) error { return nil }
func (*hostStub) DisableMissing(context.Context, []string, int64) error               { return nil }
func (h *hostStub) Due(context.Context, time.Time, int) ([]modulehost.DueTrigger, error) {
	due := h.due
	h.due = nil
	return due, nil
}
func (h *hostStub) Claim(_ context.Context, due modulehost.DueTrigger, ttl time.Duration) (schedulersdk.Run, bool, error) {
	run := schedulersdk.Run{Trigger: schedulersdk.Trigger{RunID: "run-1", DefinitionKey: due.Definition.Key, DefinitionRev: due.Definition.Revision, ScheduledFor: due.ScheduledFor, WindowKey: "window", Target: due.Definition.Target, IdempotencyKey: "scheduler:daily:window", Attempt: 1}, Status: "leased"}
	if h.lease {
		run.Lease = schedulersdk.Lease{Owner: "worker-a", Token: 1, ExpiresAt: time.Now().Add(ttl)}
	}
	return run, true, nil
}
func (h *hostStub) Renew(_ context.Context, run schedulersdk.Run, _ time.Duration) (schedulersdk.Run, bool, error) {
	h.renewed++
	return run, !h.loseLease, nil
}
func (h *hostStub) Accept(context.Context, schedulersdk.Run, schedulersdk.DownstreamReceipt) error {
	h.accepted++
	return nil
}
func (*hostStub) Fail(context.Context, schedulersdk.Run, error, time.Time) error { return nil }
func (*hostStub) List(context.Context, int) ([]schedulersdk.Run, error)          { return nil, nil }
func (*hostStub) Get(context.Context, string) (schedulersdk.Run, error) {
	return schedulersdk.Run{}, nil
}
func (*hostStub) Retry(context.Context, string, string) (schedulersdk.Run, error) {
	return schedulersdk.Run{}, nil
}
func (*hostStub) Cancel(context.Context, string, string) (schedulersdk.Run, error) {
	return schedulersdk.Run{}, nil
}
func (*hostStub) DeadLetters(context.Context, int) ([]schedulersdk.DeadLetter, error) {
	return nil, nil
}
func (*hostStub) DeadLetter(context.Context, string) (schedulersdk.DeadLetter, error) {
	return schedulersdk.DeadLetter{}, nil
}
func (*hostStub) ResolveDeadLetter(context.Context, string, string) (schedulersdk.DeadLetter, error) {
	return schedulersdk.DeadLetter{}, nil
}
func (*hostStub) RequeueDeadLetter(context.Context, string, string) (schedulersdk.Run, error) {
	return schedulersdk.Run{}, nil
}
func (*hostStub) Reschedule(context.Context, string, time.Time, string) error { return nil }
func (h *hostStub) Dispatch(ctx context.Context, _ schedulersdk.Trigger) (schedulersdk.DownstreamReceipt, error) {
	h.dispatched++
	if h.dispatchFn != nil {
		return h.dispatchFn(ctx)
	}
	return schedulersdk.DownstreamReceipt{ID: "downstream-1", Owner: "integration", Status: "accepted"}, nil
}

func TestDispatchCancelsWhenDatabaseLeaseIsLost(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	definition := schedulersdk.Definition{Key: "leased", Status: "enabled", Revision: "v1", Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60}, Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "workflow", Operation: "run"}}
	host := &hostStub{definition: definition, due: []modulehost.DueTrigger{{Definition: definition, ScheduledFor: now}}, lease: true, loseLease: true}
	host.dispatchFn = func(ctx context.Context) (schedulersdk.DownstreamReceipt, error) {
		<-ctx.Done()
		return schedulersdk.DownstreamReceipt{}, ctx.Err()
	}
	ownerCtx, cancel := context.WithCancel(t.Context())
	apiBinding := NewService(ownerCtx, cancel, schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}, host, host, host, host, schedulersdk.DeploymentModeModule)
	concrete := apiBinding
	concrete.mu.Lock()
	concrete.leaseTTL = 15 * time.Millisecond
	concrete.mu.Unlock()
	_, err := apiBinding.Tick(t.Context(), now, 1)
	if err == nil || host.renewed == 0 || host.accepted != 0 {
		t.Fatalf("err=%v renewed=%d accepted=%d", err, host.renewed, host.accepted)
	}
}
func (*hostStub) ResolveHTTPConnection(context.Context, string) (modulehost.HTTPConnection, error) {
	return modulehost.HTTPConnection{}, nil
}

func TestModuleOwnsClockButRoutesRuntimeCallbackToHostDispatcher(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	definition := schedulersdk.Definition{Key: "partner_sync", Name: "Partner sync", Status: "enabled", Revision: "v1", Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60}, Target: schedulersdk.TargetRef{Type: "http", ConnectionKey: "partner", Operation: "sync", DispatchMode: "runtime_callback"}}
	host := &hostStub{definition: definition, due: []modulehost.DueTrigger{{Definition: definition, ScheduledFor: now}}}
	ownerCtx, cancel := context.WithCancel(t.Context())
	binding := NewService(ownerCtx, cancel, schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}, host, host, host, host, schedulersdk.DeploymentModeModule)
	if err := binding.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	processed, err := binding.Tick(t.Context(), now, 10)
	if err != nil || processed != 1 || host.dispatched != 1 || host.accepted != 1 {
		t.Fatalf("processed=%d dispatched=%d accepted=%d err=%v", processed, host.dispatched, host.accepted, err)
	}
}
