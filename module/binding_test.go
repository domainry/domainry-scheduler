package module

import (
	"context"
	"testing"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
)

type hostStub struct {
	definition schedulersdk.Definition
	due        []modulehost.DueTrigger
	dispatched int
	accepted   int
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
func (*hostStub) Claim(_ context.Context, due modulehost.DueTrigger, _ time.Duration) (schedulersdk.Run, bool, error) {
	return schedulersdk.Run{Trigger: schedulersdk.Trigger{RunID: "run-1", DefinitionKey: due.Definition.Key, DefinitionRev: due.Definition.Revision, ScheduledFor: due.ScheduledFor, WindowKey: "window", Target: due.Definition.Target, IdempotencyKey: "scheduler:daily:window", Attempt: 1}, Status: "leased"}, true, nil
}
func (h *hostStub) Accept(context.Context, schedulersdk.Run, schedulersdk.DownstreamReceipt) error {
	h.accepted++
	return nil
}
func (*hostStub) Fail(context.Context, schedulersdk.Run, error, time.Time) error { return nil }
func (*hostStub) List(context.Context, int) ([]schedulersdk.Run, error)          { return nil, nil }
func (h *hostStub) Dispatch(context.Context, schedulersdk.Trigger) (schedulersdk.DownstreamReceipt, error) {
	h.dispatched++
	return schedulersdk.DownstreamReceipt{ID: "downstream-1", Owner: "integration", Status: "accepted"}, nil
}
func (*hostStub) ResolveHTTPConnection(context.Context, string) (modulehost.HTTPConnection, error) {
	return modulehost.HTTPConnection{}, nil
}

func TestModuleOwnsClockButRoutesRuntimeCallbackToHostDispatcher(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	definition := schedulersdk.Definition{Key: "partner_sync", Name: "Partner sync", Status: "enabled", Revision: "v1", Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60}, Target: schedulersdk.TargetRef{Type: "http", ConnectionKey: "partner", Operation: "sync", DispatchMode: "runtime_callback"}}
	host := &hostStub{definition: definition, due: []modulehost.DueTrigger{{Definition: definition, ScheduledFor: now}}}
	binding, err := NewFactory(Options{}).OpenModule(t.Context(), schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}, host)
	if err != nil {
		t.Fatal(err)
	}
	if err := binding.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	processed, err := binding.Tick(t.Context(), now, 10)
	if err != nil || processed != 1 || host.dispatched != 1 || host.accepted != 1 {
		t.Fatalf("processed=%d dispatched=%d accepted=%d err=%v", processed, host.dispatched, host.accepted, err)
	}
}
