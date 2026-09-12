package saas

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
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
	session     schedulersdk.DefinitionPublisherSession
	calls       int
	application schedulersdk.ApplicationRef
	reschedules int
	closeCalls  int
	plan        schedulersdk.ScheduledPlan
}

type handlerRoundTripper struct{ handler http.Handler }

func (t handlerRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	response := httptest.NewRecorder()
	t.handler.ServeHTTP(response, request)
	return response.Result(), nil
}

func (s *serviceStub) called(application schedulersdk.ApplicationRef) {
	s.calls++
	s.application = application
}

func (s *serviceStub) Descriptor(_ context.Context, application schedulersdk.ApplicationRef) (schedulersdk.Descriptor, error) {
	s.called(application)
	return schedulersdk.Descriptor{ProtocolVersion: schedulersdk.ProtocolVersionV1, Mode: schedulersdk.DeploymentModeSaaS, Capabilities: []string{schedulersdk.CapabilityDefinitionPublicationFencing}}, nil
}
func (s *serviceStub) BeginDefinitionPublisherSession(_ context.Context, application schedulersdk.ApplicationRef) (schedulersdk.DefinitionPublisherSession, error) {
	s.called(application)
	if s.session.Generation == 0 {
		s.session = schedulersdk.DefinitionPublisherSession{ContractVersion: schedulersdk.DefinitionPublicationContractVersion, Generation: 1, SessionNonce: strings.Repeat("s", 32)}
	}
	return s.session, nil
}
func (s *serviceStub) Reconcile(_ context.Context, application schedulersdk.ApplicationRef, snapshot schedulersdk.DefinitionSnapshot) error {
	s.called(application)
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
func (s *serviceStub) Reschedule(context.Context, schedulersdk.ApplicationRef, string, time.Time, string) error {
	s.reschedules++
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
func (s *serviceStub) DeadLetters(_ context.Context, application schedulersdk.ApplicationRef, _ int) ([]schedulersdk.DeadLetter, error) {
	s.called(application)
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
func (s *serviceStub) Close(context.Context, schedulersdk.ApplicationRef) error {
	s.closeCalls++
	return nil
}
func (s *serviceStub) CreateScheduledPlan(_ context.Context, application schedulersdk.ApplicationRef, input schedulersdk.ScheduledPlanCreate) (schedulersdk.ScheduledPlanReceipt, error) {
	s.called(application)
	s.plan = schedulersdk.ScheduledPlan{ID: "plan-saas", Name: input.Name, Owner: input.Owner, Timezone: input.Timezone, Trigger: input.Trigger, Input: input.Input, AllowedActions: input.AllowedActions, Target: input.Target, ConversationRef: input.ConversationRef, Status: "enabled", Revision: 1}
	return schedulersdk.ScheduledPlanReceipt{Plan: s.plan}, nil
}
func (s *serviceStub) GetScheduledPlan(_ context.Context, application schedulersdk.ApplicationRef, lookup schedulersdk.ScheduledPlanLookup) (schedulersdk.ScheduledPlan, error) {
	s.called(application)
	if lookup.PlanID != s.plan.ID || lookup.Owner != s.plan.Owner {
		return schedulersdk.ScheduledPlan{}, schedulersdk.ErrScheduledPlanNotFound
	}
	return s.plan, nil
}
func (s *serviceStub) ListScheduledPlans(_ context.Context, application schedulersdk.ApplicationRef, input schedulersdk.ScheduledPlanList) (schedulersdk.ScheduledPlanPage, error) {
	s.called(application)
	if input.Owner != s.plan.Owner {
		return schedulersdk.ScheduledPlanPage{Items: []schedulersdk.ScheduledPlan{}}, nil
	}
	return schedulersdk.ScheduledPlanPage{Items: []schedulersdk.ScheduledPlan{s.plan}}, nil
}
func (s *serviceStub) UpdateScheduledPlan(_ context.Context, application schedulersdk.ApplicationRef, input schedulersdk.ScheduledPlanUpdate) (schedulersdk.ScheduledPlanReceipt, error) {
	s.called(application)
	s.plan.Name, s.plan.Revision = input.Name, input.ExpectedRevision+1
	return schedulersdk.ScheduledPlanReceipt{Plan: s.plan}, nil
}
func (s *serviceStub) PauseScheduledPlan(_ context.Context, application schedulersdk.ApplicationRef, input schedulersdk.ScheduledPlanStatusChange) (schedulersdk.ScheduledPlanReceipt, error) {
	s.called(application)
	s.plan.Status, s.plan.Revision = schedulersdk.ScheduledPlanStatusPaused, input.ExpectedRevision+1
	return schedulersdk.ScheduledPlanReceipt{Plan: s.plan}, nil
}
func (s *serviceStub) ResumeScheduledPlan(_ context.Context, application schedulersdk.ApplicationRef, input schedulersdk.ScheduledPlanStatusChange) (schedulersdk.ScheduledPlanReceipt, error) {
	s.called(application)
	s.plan.Status, s.plan.Revision = schedulersdk.ScheduledPlanStatusEnabled, input.ExpectedRevision+1
	return schedulersdk.ScheduledPlanReceipt{Plan: s.plan}, nil
}
func (s *serviceStub) DeleteScheduledPlan(_ context.Context, application schedulersdk.ApplicationRef, input schedulersdk.ScheduledPlanStatusChange) (schedulersdk.ScheduledPlanDeleteReceipt, error) {
	s.called(application)
	return schedulersdk.ScheduledPlanDeleteReceipt{PlanID: input.PlanID, Revision: input.ExpectedRevision + 1, Deleted: true}, nil
}

func TestServerAndSDKHTTPTransportPublishDefinitions(t *testing.T) {
	service := &serviceStub{}
	handler, err := New(Options{ApplicationTokens: map[string]string{"runtime-a": "secret"}, Service: service})
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: handlerRoundTripper{handler: handler.Routes()}}
	directCapability, err := schedulercapability.NewBinding()
	if err != nil {
		t.Fatal(err)
	}
	directSummary, err := directCapability.CapabilitySummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	transport, err := httptransport.Open(t.Context(), httptransport.Config{Endpoint: "http://scheduler.test", Token: "secret", Client: client, CapabilityContractSHA256: directSummary.Identity.ContractSHA256})
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
	if _, err := httptransport.Open(t.Context(), httptransport.Config{Endpoint: "http://scheduler.test", Token: "secret", Client: client, CapabilityContractSHA256: strings.Repeat("0", 64)}); err == nil {
		t.Fatal("Scheduler Remote accepted a stale capability digest")
	}
	session, err := transport.BeginDefinitionPublisherSession(t.Context(), schedulersdk.ApplicationRef{RuntimeID: "runtime-a"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := schedulersdk.DefinitionSnapshot{PublisherSession: &session, Revision: 9, Definitions: []schedulersdk.Definition{{Key: "daily"}}}
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

func TestServerAndSDKTransportPreserveScheduledPlanOwnerScope(t *testing.T) {
	service := &serviceStub{}
	handler, err := New(Options{ApplicationTokens: map[string]string{"runtime-a": "secret"}, Service: service})
	if err != nil {
		t.Fatal(err)
	}
	directCapability, _ := schedulercapability.NewBinding()
	summary, _ := directCapability.CapabilitySummary(t.Context())
	transport, err := httptransport.Open(t.Context(), httptransport.Config{Endpoint: "http://scheduler.test", Token: "secret", Client: &http.Client{Transport: handlerRoundTripper{handler: handler.Routes()}}, CapabilityContractSHA256: summary.Identity.ContractSHA256})
	if err != nil {
		t.Fatal(err)
	}
	application := schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}
	owner := schedulersdk.ScheduledPlanOwner{WorkspaceID: "workspace-a", UserID: "user-a", ProductKey: "agent"}
	at := time.Date(2026, 9, 18, 1, 0, 0, 0, time.UTC)
	created, err := transport.CreateScheduledPlan(t.Context(), application, schedulersdk.ScheduledPlanCreate{ClientID: "friday", Name: "Friday reminder", Owner: owner, Timezone: "Asia/Shanghai", Trigger: schedulersdk.ScheduledPlanTrigger{Type: "once", At: &at}, Input: json.RawMessage(`{"status":"open"}`), AllowedActions: []string{"todo.list"}, Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "agent", Operation: "conversation_task_start"}, ConversationRef: schedulersdk.ScheduledPlanConversationRef{ConversationID: "conversation-a"}})
	if err != nil || created.Plan.ID != "plan-saas" || service.application.RuntimeID != "runtime-a" {
		t.Fatalf("created=%+v application=%+v err=%v", created, service.application, err)
	}
	loaded, err := transport.GetScheduledPlan(t.Context(), application, schedulersdk.ScheduledPlanLookup{Owner: owner, PlanID: created.Plan.ID})
	if err != nil || loaded.ID != created.Plan.ID || loaded.ConversationRef.ConversationID != "conversation-a" {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	other := owner
	other.UserID = "user-b"
	if _, err := transport.GetScheduledPlan(t.Context(), application, schedulersdk.ScheduledPlanLookup{Owner: other, PlanID: created.Plan.ID}); !errors.Is(err, schedulersdk.ErrScheduledPlanNotFound) {
		t.Fatalf("cross-user err=%v", err)
	}
}

func TestServerBindsPrivateCredentialToRuntimeBeforeCallingService(t *testing.T) {
	service := &serviceStub{}
	handler, err := New(Options{ApplicationTokens: map[string]string{"runtime-a": "token-a", "runtime-b": "token-b"}, Service: service})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, path, token string
		want              int
	}{
		{name: "cross tenant", path: "/v1/applications/runtime-b/descriptor", token: "token-a", want: http.StatusForbidden},
		{name: "unknown tenant", path: "/v1/applications/runtime-unknown/descriptor", token: "token-a", want: http.StatusForbidden},
		{name: "wrong token", path: "/v1/applications/runtime-a/descriptor", token: "wrong", want: http.StatusUnauthorized},
		{name: "missing token", path: "/v1/applications/runtime-a/descriptor", want: http.StatusUnauthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, test.path, nil)
			if test.token != "" {
				request.Header.Set("Authorization", "Bearer "+test.token)
			}
			request.Header.Set("X-Domainry-Runtime-ID", "runtime-b")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.want || service.calls != 0 {
				t.Fatalf("status=%d want=%d service calls=%d body=%s", response.Code, test.want, service.calls, response.Body.String())
			}
		})
	}
	request := httptest.NewRequest(http.MethodGet, "/v1/applications/runtime-a/descriptor", nil)
	request.Header.Set("Authorization", "Bearer token-a")
	request.Header.Set("X-Domainry-Runtime-ID", "runtime-b")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || service.calls != 1 || service.application.RuntimeID != "runtime-a" {
		t.Fatalf("status=%d calls=%d application=%+v body=%s", response.Code, service.calls, service.application, response.Body.String())
	}
}

func TestServerDoesNotMountPublicSchedulerActionsWithoutHumanPrincipalAuthorization(t *testing.T) {
	service := &serviceStub{}
	handler, err := New(Options{ApplicationTokens: map[string]string{"runtime-a": "token-a"}, Service: service})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ method, path, body string }{
		{method: http.MethodGet, path: "/scheduler/state"},
		{method: http.MethodGet, path: "/scheduler/definitions/daily"},
		{method: http.MethodPost, path: "/scheduler/definitions/daily/run", body: `{}`},
	} {
		request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
		request.Header.Set("Authorization", "Bearer token-a")
		request.Header.Set("X-Domainry-Runtime-ID", "runtime-a")
		request.Header.Set("Idempotency-Key", "attack-1")
		request.Header.Set("X-Operation-Reason", "attack")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound || service.calls != 0 {
			t.Fatalf("%s %s status=%d service calls=%d body=%s", test.method, test.path, response.Code, service.calls, response.Body.String())
		}
	}
}

func TestServerRoutesRescheduleSeparatelyFromPublicationSession(t *testing.T) {
	service := &serviceStub{}
	handler, err := New(Options{ApplicationTokens: map[string]string{"runtime-a": "token-a"}, Service: service})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/applications/runtime-a/definitions/daily/reschedule", strings.NewReader(`{"next_run_at":"2026-09-08T09:00:00Z","reason":"operator"}`))
	request.Header.Set("Authorization", "Bearer token-a")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || service.reschedules != 1 {
		t.Fatalf("reschedule status=%d calls=%d body=%s", response.Code, service.reschedules, response.Body.String())
	}

	weird := httptest.NewRequest(http.MethodPost, "/v1/applications/runtime-a/definition-publication-sessions/daily/reschedule", strings.NewReader(`{"next_run_at":"2026-09-08T09:00:00Z","reason":"operator"}`))
	weird.Header.Set("Authorization", "Bearer token-a")
	weirdResponse := httptest.NewRecorder()
	handler.ServeHTTP(weirdResponse, weird)
	if weirdResponse.Code != http.StatusMethodNotAllowed || service.reschedules != 1 || service.session.Generation != 0 {
		t.Fatalf("weird path status=%d reschedules=%d session=%+v body=%s", weirdResponse.Code, service.reschedules, service.session, weirdResponse.Body.String())
	}
}

func TestLegacyBindingDeleteIsAServiceLifecycleNoop(t *testing.T) {
	service := &serviceStub{}
	handler, err := New(Options{ApplicationTokens: map[string]string{"runtime-a": "token-a"}, Service: service})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodDelete, "/v1/applications/runtime-a/binding", nil)
	request.Header.Set("Authorization", "Bearer token-a")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || service.closeCalls != 0 {
		t.Fatalf("binding delete status=%d service close calls=%d", response.Code, service.closeCalls)
	}
}

func TestServerRejectsCredentialsThatAreMissingOrSharedAcrossApplications(t *testing.T) {
	service := &serviceStub{}
	if _, err := New(Options{Service: service}); err == nil {
		t.Fatal("server accepted no application credentials")
	}
	if _, err := New(Options{ApplicationTokens: map[string]string{"runtime-a": "shared", "runtime-b": "shared"}, Service: service}); err == nil {
		t.Fatal("server accepted one bearer credential for multiple applications")
	}
	if _, err := New(Options{ApplicationTokens: map[string]string{"runtime-a": "  "}, Service: service}); err == nil {
		t.Fatal("server accepted an empty bearer credential")
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
