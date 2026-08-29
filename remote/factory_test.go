package remote

import (
	"context"
	"testing"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
)

type definitionProvider struct {
	snapshot schedulersdk.DefinitionSnapshot
}

func (p definitionProvider) Snapshot(context.Context) (schedulersdk.DefinitionSnapshot, error) {
	return p.snapshot, nil
}

type dispatcherStub struct{}

func (dispatcherStub) Dispatch(context.Context, schedulersdk.Trigger) (schedulersdk.DownstreamReceipt, error) {
	return schedulersdk.DownstreamReceipt{}, nil
}

type hostStub struct{ definitions definitionProvider }

func (h hostStub) Definitions() modulehost.DefinitionProvider       { return h.definitions }
func (hostStub) Runs() modulehost.RunStore                          { return nil }
func (hostStub) Dispatcher() modulehost.Dispatcher                  { return dispatcherStub{} }
func (hostStub) HTTPConnections() modulehost.HTTPConnectionProvider { return nil }

type transportStub struct {
	snapshot schedulersdk.DefinitionSnapshot
}

func (*transportStub) Descriptor(context.Context, schedulersdk.ApplicationRef) (schedulersdk.Descriptor, error) {
	return schedulersdk.Descriptor{ProtocolVersion: schedulersdk.ProtocolVersionV1, Mode: schedulersdk.DeploymentModeSaaS}, nil
}
func (t *transportStub) Reconcile(_ context.Context, _ schedulersdk.ApplicationRef, snapshot schedulersdk.DefinitionSnapshot) error {
	t.snapshot = snapshot
	return nil
}
func (*transportStub) Preview(context.Context, schedulersdk.ApplicationRef, schedulersdk.Schedule, time.Time, int) ([]time.Time, error) {
	return nil, nil
}
func (*transportStub) Tick(context.Context, schedulersdk.ApplicationRef, time.Time, int) (int, error) {
	return 0, nil
}
func (*transportStub) TriggerNow(context.Context, schedulersdk.ApplicationRef, string, string) (schedulersdk.Run, error) {
	return schedulersdk.Run{}, nil
}
func (*transportStub) Runs(context.Context, schedulersdk.ApplicationRef, int) ([]schedulersdk.Run, error) {
	return nil, nil
}
func (*transportStub) Close(context.Context, schedulersdk.ApplicationRef) error { return nil }

func TestSaaSBindingPublishesRuntimeDefinitionSnapshot(t *testing.T) {
	definition := schedulersdk.Definition{Key: "daily", Status: "enabled", Revision: "v1", Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60}, Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "workflow", Operation: "scheduled:daily"}}
	transport := &transportStub{}
	binding, err := NewFactory(transport).OpenSaaS(t.Context(), schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}, hostStub{definitions: definitionProvider{snapshot: schedulersdk.DefinitionSnapshot{Revision: 11, Definitions: []schedulersdk.Definition{definition}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := binding.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if transport.snapshot.Revision != 11 || len(transport.snapshot.Definitions) != 1 {
		t.Fatalf("snapshot=%#v", transport.snapshot)
	}
}
