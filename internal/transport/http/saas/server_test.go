package saas

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/saashost/httptransport"
)

type serviceStub struct {
	snapshot schedulersdk.DefinitionSnapshot
}

func (s *serviceStub) Descriptor(context.Context, schedulersdk.ApplicationRef) (schedulersdk.Descriptor, error) {
	return schedulersdk.Descriptor{ProtocolVersion: schedulersdk.ProtocolVersionV1, Mode: schedulersdk.DeploymentModeSaaS}, nil
}
func (s *serviceStub) Reconcile(_ context.Context, _ schedulersdk.ApplicationRef, snapshot schedulersdk.DefinitionSnapshot) error {
	s.snapshot = snapshot
	return nil
}
func (*serviceStub) Preview(context.Context, schedulersdk.ApplicationRef, schedulersdk.Schedule, time.Time, int) ([]time.Time, error) {
	return nil, nil
}
func (*serviceStub) Tick(context.Context, schedulersdk.ApplicationRef, time.Time, int) (int, error) {
	return 0, nil
}
func (*serviceStub) TriggerNow(context.Context, schedulersdk.ApplicationRef, string, string) (schedulersdk.Run, error) {
	return schedulersdk.Run{}, nil
}
func (*serviceStub) Reschedule(context.Context, schedulersdk.ApplicationRef, string, time.Time, string) error {
	return nil
}
func (*serviceStub) Runs(context.Context, schedulersdk.ApplicationRef, int) ([]schedulersdk.Run, error) {
	return nil, nil
}
func (*serviceStub) Run(context.Context, schedulersdk.ApplicationRef, string) (schedulersdk.Run, error) {
	return schedulersdk.Run{}, nil
}
func (*serviceStub) RetryRun(context.Context, schedulersdk.ApplicationRef, string, string) (schedulersdk.Run, error) {
	return schedulersdk.Run{}, nil
}
func (*serviceStub) CancelRun(context.Context, schedulersdk.ApplicationRef, string, string) (schedulersdk.Run, error) {
	return schedulersdk.Run{}, nil
}
func (*serviceStub) DeadLetter(context.Context, schedulersdk.ApplicationRef, string) (schedulersdk.DeadLetter, error) {
	return schedulersdk.DeadLetter{}, nil
}
func (*serviceStub) ResolveDeadLetter(context.Context, schedulersdk.ApplicationRef, string, string) (schedulersdk.DeadLetter, error) {
	return schedulersdk.DeadLetter{}, nil
}
func (*serviceStub) RequeueDeadLetter(context.Context, schedulersdk.ApplicationRef, string, string) (schedulersdk.Run, error) {
	return schedulersdk.Run{}, nil
}
func (*serviceStub) Close(context.Context, schedulersdk.ApplicationRef) error { return nil }

func TestServerAndSDKHTTPTransportPublishDefinitions(t *testing.T) {
	service := &serviceStub{}
	httpServer := httptest.NewServer(New(Options{BearerToken: "secret", Service: service}).Routes())
	defer httpServer.Close()
	transport, err := httptransport.New(httptransport.Config{Endpoint: httpServer.URL, Token: "secret", Client: httpServer.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if descriptor, err := transport.Descriptor(t.Context(), schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}); err != nil || descriptor.Mode != schedulersdk.DeploymentModeSaaS {
		t.Fatalf("descriptor=%#v err=%v", descriptor, err)
	}
	snapshot := schedulersdk.DefinitionSnapshot{Revision: 9, Definitions: []schedulersdk.Definition{{Key: "daily"}}}
	if err := transport.Reconcile(t.Context(), schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}, snapshot); err != nil {
		t.Fatal(err)
	}
	if service.snapshot.Revision != 9 || len(service.snapshot.Definitions) != 1 {
		t.Fatalf("snapshot=%#v", service.snapshot)
	}
}
