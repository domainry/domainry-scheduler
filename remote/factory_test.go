package remote

import (
	"context"
	"testing"
	"time"

	"github.com/domainry/domainry-foundation/modulecapability"
	"github.com/domainry/domainry-foundation/modulecapability/contracttest"
	"github.com/domainry/domainry-foundation/modulehttp"
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
	modulecapability.Binding
	snapshot schedulersdk.DefinitionSnapshot
	session  schedulersdk.DefinitionPublisherSession
	closes   int
	bound    schedulersdk.ApplicationRef
}

func (*transportStub) Descriptor(context.Context, schedulersdk.ApplicationRef) (schedulersdk.Descriptor, error) {
	return schedulersdk.Descriptor{ProtocolVersion: schedulersdk.ProtocolVersionV1, Mode: schedulersdk.DeploymentModeSaaS, Capabilities: []string{schedulersdk.CapabilityDefinitionPublicationFencing}}, nil
}
func (t *transportStub) BeginDefinitionPublisherSession(context.Context, schedulersdk.ApplicationRef) (schedulersdk.DefinitionPublisherSession, error) {
	if t.session.Generation == 0 {
		t.session = schedulersdk.DefinitionPublisherSession{ContractVersion: schedulersdk.DefinitionPublicationContractVersion, Generation: 1, SessionNonce: "0123456789abcdef0123456789abcdef"}
	}
	return t.session, nil
}
func (t *transportStub) BindApplication(application schedulersdk.ApplicationRef) error {
	t.bound = application
	return nil
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
func (*transportStub) Reschedule(context.Context, schedulersdk.ApplicationRef, string, time.Time, string) error {
	return nil
}
func (*transportStub) Runs(context.Context, schedulersdk.ApplicationRef, int) ([]schedulersdk.Run, error) {
	return nil, nil
}
func (*transportStub) Run(context.Context, schedulersdk.ApplicationRef, string) (schedulersdk.Run, error) {
	return schedulersdk.Run{}, nil
}
func (*transportStub) RetryRun(context.Context, schedulersdk.ApplicationRef, string, string) (schedulersdk.Run, error) {
	return schedulersdk.Run{}, nil
}
func (*transportStub) CancelRun(context.Context, schedulersdk.ApplicationRef, string, string) (schedulersdk.Run, error) {
	return schedulersdk.Run{}, nil
}
func (*transportStub) DeadLetters(context.Context, schedulersdk.ApplicationRef, int) ([]schedulersdk.DeadLetter, error) {
	return nil, nil
}
func (*transportStub) DeadLetter(context.Context, schedulersdk.ApplicationRef, string) (schedulersdk.DeadLetter, error) {
	return schedulersdk.DeadLetter{}, nil
}
func (*transportStub) ResolveDeadLetter(context.Context, schedulersdk.ApplicationRef, string, string) (schedulersdk.DeadLetter, error) {
	return schedulersdk.DeadLetter{}, nil
}
func (*transportStub) RequeueDeadLetter(context.Context, schedulersdk.ApplicationRef, string, string) (schedulersdk.Run, error) {
	return schedulersdk.Run{}, nil
}
func (t *transportStub) Close(context.Context, schedulersdk.ApplicationRef) error {
	t.closes++
	return nil
}

func TestSaaSBindingPublishesRuntimeDefinitionSnapshot(t *testing.T) {
	definition := schedulersdk.Definition{Key: "daily", Status: "enabled", Revision: "v1", Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60}, Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "workflow", Operation: "scheduled:daily"}}
	capability, err := contracttest.NewFixtureBinding("scheduler")
	if err != nil {
		t.Fatal(err)
	}
	transport := &transportStub{Binding: capability}
	binding, err := NewFactory(transport).OpenSaaS(t.Context(), schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}, hostStub{definitions: definitionProvider{snapshot: schedulersdk.DefinitionSnapshot{Revision: 11, Definitions: []schedulersdk.Definition{definition}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := binding.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, republishesSchedulerRoutes := binding.(modulehttp.Provider); republishesSchedulerRoutes {
		t.Fatal("Scheduler remote binding must not republish Scheduler-owned HTTP routes through Runtime")
	}
	if transport.bound.RuntimeID != "runtime-a" || transport.snapshot.Revision != 11 || len(transport.snapshot.Definitions) != 1 || transport.snapshot.PublisherSession == nil || transport.snapshot.PublisherSession.Generation != transport.session.Generation {
		t.Fatalf("snapshot=%#v", transport.snapshot)
	}
	if err := binding.Close(t.Context()); err != nil || transport.closes != 0 {
		t.Fatalf("close err=%v transport calls=%d", err, transport.closes)
	}
}

func TestSaaSBindingRejectsInvalidPublisherSession(t *testing.T) {
	capability, err := contracttest.NewFixtureBinding("scheduler")
	if err != nil {
		t.Fatal(err)
	}
	transport := &transportStub{
		Binding: capability,
		session: schedulersdk.DefinitionPublisherSession{ContractVersion: schedulersdk.DefinitionPublicationContractVersion, Generation: 1},
	}
	_, err = NewFactory(transport).OpenSaaS(t.Context(), schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}, hostStub{definitions: definitionProvider{}})
	if err == nil {
		t.Fatal("Scheduler remote accepted an invalid publisher session")
	}
}

func TestSaaSBindingStartTracksEveryCallerContextAndClose(t *testing.T) {
	capability, err := contracttest.NewFixtureBinding("scheduler")
	if err != nil {
		t.Fatal(err)
	}
	transport := &transportStub{Binding: capability}
	binding, err := NewFactory(transport).OpenSaaS(t.Context(), schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}, hostStub{definitions: definitionProvider{}})
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, cancelFirst := context.WithCancel(t.Context())
	secondCtx, cancelSecond := context.WithCancel(t.Context())
	firstDone := binding.Start(firstCtx, schedulersdk.WorkerConfig{Enabled: true})
	secondDone := binding.Start(secondCtx, schedulersdk.WorkerConfig{Enabled: true})
	for name, done := range map[string]<-chan struct{}{"first": firstDone, "second": secondDone} {
		select {
		case <-done:
			t.Fatalf("remote %s Start completed before its context", name)
		default:
		}
	}
	cancelFirst()
	cancelFirst()
	waitRemoteDone(t, firstDone, "first Start context cancellation")
	select {
	case <-secondDone:
		t.Fatal("first Start cancellation stopped the independent second Start")
	default:
	}
	cancelSecond()
	waitRemoteDone(t, secondDone, "second Start context cancellation")

	closeDone := binding.Start(context.Background(), schedulersdk.WorkerConfig{Enabled: true})
	if err := binding.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	waitRemoteDone(t, closeDone, "remote binding Close")
	if err := binding.Close(t.Context()); err != nil {
		t.Fatalf("repeated close: %v", err)
	}
	waitRemoteDone(t, binding.Start(context.Background(), schedulersdk.WorkerConfig{Enabled: true}), "post-Close Start")
	if transport.closes != 0 {
		t.Fatalf("Runtime-side binding close reached Scheduler-owned transport %d times", transport.closes)
	}
}

func waitRemoteDone(t *testing.T, done <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}
