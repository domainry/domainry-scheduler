package saas

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/domainry/domainry-foundation/modulecapability"
	"github.com/domainry/domainry-foundation/modulecapability/contracttest"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/saashost/httptransport"
	schedulercapability "github.com/domainry/domainry-scheduler/internal/capability"
)

type serviceStub struct {
	snapshot    schedulersdk.DefinitionSnapshot
	deadLetters []schedulersdk.DeadLetter
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
func (s *serviceStub) DeadLetters(context.Context, schedulersdk.ApplicationRef, int) ([]schedulersdk.DeadLetter, error) {
	return s.deadLetters, nil
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
	handler, err := New(Options{BearerToken: "secret", Service: service})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(handler.Routes())
	defer httpServer.Close()
	directCapability, err := schedulercapability.NewBinding()
	if err != nil {
		t.Fatal(err)
	}
	directSummary, err := directCapability.CapabilitySummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	transport, err := httptransport.Open(t.Context(), httptransport.Config{Endpoint: httpServer.URL, Token: "secret", Client: httpServer.Client(), CapabilityContractSHA256: directSummary.Identity.ContractSHA256})
	if err != nil {
		t.Fatal(err)
	}
	contracttest.VerifyBinding(t, transport)
	if descriptor, err := transport.Descriptor(t.Context(), schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}); err != nil || descriptor.Mode != schedulersdk.DeploymentModeSaaS {
		t.Fatalf("descriptor=%#v err=%v", descriptor, err)
	}
	remoteSummary, err := transport.CapabilitySummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	assertCanonicalEqual(t, directSummary, remoteSummary)
	for _, category := range directSummary.Categories {
		directDocument, directErr := directCapability.CapabilityCategory(t.Context(), category.Key)
		remoteDocument, remoteErr := transport.CapabilityCategory(t.Context(), category.Key)
		if directErr != nil || remoteErr != nil {
			t.Fatalf("capability category %q: direct=%v remote=%v", category.Key, directErr, remoteErr)
		}
		assertCanonicalEqual(t, directDocument, remoteDocument)
	}
	if _, err := httptransport.Open(t.Context(), httptransport.Config{Endpoint: httpServer.URL, Token: "secret", Client: httpServer.Client(), CapabilityContractSHA256: strings.Repeat("0", 64)}); err == nil {
		t.Fatal("Scheduler Remote accepted a stale capability digest")
	}
	snapshot := schedulersdk.DefinitionSnapshot{Revision: 9, Definitions: []schedulersdk.Definition{{Key: "daily"}}}
	if err := transport.Reconcile(t.Context(), schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}, snapshot); err != nil {
		t.Fatal(err)
	}
	if service.snapshot.Revision != 9 || len(service.snapshot.Definitions) != 1 {
		t.Fatalf("snapshot=%#v", service.snapshot)
	}
	service.deadLetters = []schedulersdk.DeadLetter{{RunID: "run-dead", DefinitionKey: "daily", Status: "open"}}
	deadLetters, err := transport.DeadLetters(t.Context(), schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}, 25)
	if err != nil {
		t.Fatal(err)
	}
	if len(deadLetters) != 1 || deadLetters[0].RunID != "run-dead" {
		t.Fatalf("dead letters=%+v", deadLetters)
	}
}

func assertCanonicalEqual(t *testing.T, left, right any) {
	t.Helper()
	leftBytes, err := modulecapability.CanonicalJSON(left)
	if err != nil {
		t.Fatal(err)
	}
	rightBytes, err := modulecapability.CanonicalJSON(right)
	if err != nil {
		t.Fatal(err)
	}
	if string(leftBytes) != string(rightBytes) {
		t.Fatalf("canonical mismatch\nleft=%s\nright=%s", leftBytes, rightBytes)
	}
}
