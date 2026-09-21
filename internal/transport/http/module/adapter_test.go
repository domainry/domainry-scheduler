package module

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/domainry/domainry-foundation/modulehttp"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/persistence"
	schedulermodel "github.com/domainry/domainry-scheduler/internal/domain/scheduler/model"
)

type definitionRepositoryStub struct {
	snapshot persistence.DefinitionSnapshot
}

func (*definitionRepositoryStub) SyncDefinitions(context.Context, persistence.DefinitionSnapshot) error {
	return nil
}
func (r *definitionRepositoryStub) DefinitionSnapshot(context.Context) (persistence.DefinitionSnapshot, error) {
	return r.snapshot, nil
}

type bindingStub struct {
	repository      *definitionRepositoryStub
	runs            []schedulersdk.Run
	deadLetters     []schedulersdk.DeadLetter
	commandReason   string
	commandResource string
	rescheduledAt   time.Time
	triggeredRun    schedulersdk.Run
	retriedRun      schedulersdk.Run
	cancelledRun    schedulersdk.Run
	resolvedLetter  schedulersdk.DeadLetter
	requeuedRun     schedulersdk.Run
	commandReceipts map[string]schedulermodel.CommandReceipt
	triggerCalls    int
}

func (b *bindingStub) DefinitionRepository() persistence.DefinitionRepository { return b.repository }
func (*bindingStub) Preview(_ context.Context, _ schedulersdk.Schedule, after time.Time, count int) ([]time.Time, error) {
	result := make([]time.Time, count)
	for index := range result {
		result[index] = after.Add(time.Duration(index+1) * time.Hour)
	}
	return result, nil
}
func (b *bindingStub) TriggerNow(_ context.Context, key, reason string) (schedulersdk.Run, error) {
	b.triggerCalls++
	b.commandResource, b.commandReason = key, reason
	return b.triggeredRun, nil
}
func (b *bindingStub) Reschedule(_ context.Context, key string, value time.Time, reason string) error {
	b.commandResource, b.commandReason, b.rescheduledAt = key, reason, value
	return nil
}
func (b *bindingStub) Runs(context.Context, int) ([]schedulersdk.Run, error) { return b.runs, nil }
func (b *bindingStub) Run(context.Context, string) (schedulersdk.Run, error) {
	return b.triggeredRun, nil
}
func (b *bindingStub) RetryRun(_ context.Context, id, reason string) (schedulersdk.Run, error) {
	b.commandResource, b.commandReason = id, reason
	return b.retriedRun, nil
}
func (b *bindingStub) CancelRun(_ context.Context, id, reason string) (schedulersdk.Run, error) {
	b.commandResource, b.commandReason = id, reason
	return b.cancelledRun, nil
}
func (b *bindingStub) DeadLetters(context.Context, int) ([]schedulersdk.DeadLetter, error) {
	return b.deadLetters, nil
}
func (b *bindingStub) ResolveDeadLetter(_ context.Context, id, reason string) (schedulersdk.DeadLetter, error) {
	b.commandResource, b.commandReason = id, reason
	return b.resolvedLetter, nil
}
func (b *bindingStub) RequeueDeadLetter(_ context.Context, id, reason string) (schedulersdk.Run, error) {
	b.commandResource, b.commandReason = id, reason
	return b.requeuedRun, nil
}
func (b *bindingStub) ClaimCommand(_ context.Context, receipt schedulermodel.CommandReceipt) (schedulermodel.CommandReceipt, bool, error) {
	if existing, found := b.commandReceipts[receipt.IdempotencyKey]; found {
		return existing, false, nil
	}
	receipt.Status = schedulermodel.CommandReceiptExecuting
	b.commandReceipts[receipt.IdempotencyKey] = receipt
	return receipt, true, nil
}
func (b *bindingStub) CompleteCommand(_ context.Context, key, requestHash string, status int, body []byte) error {
	receipt := b.commandReceipts[key]
	receipt.RequestHash = requestHash
	receipt.Status = schedulermodel.CommandReceiptCompleted
	receipt.HTTPStatus = status
	receipt.ResponseJSON = append([]byte(nil), body...)
	b.commandReceipts[key] = receipt
	return nil
}

func TestAdapterPublishesAndImplementsEverySchedulerAction(t *testing.T) {
	binding := schedulerBindingFixture()
	value, err := NewAdapter(binding, binding)
	if err != nil {
		t.Fatal(err)
	}
	if err := modulehttp.ValidateAdapter(value); err != nil {
		t.Fatal(err)
	}
	routes := value.Routes()
	if len(routes) != 13 {
		t.Fatalf("routes=%d", len(routes))
	}
	openAPI, ok := value.(modulehttp.OpenAPIProvider)
	if !ok || len(openAPI.OpenAPIOperations()) != len(routes) {
		t.Fatalf("OpenAPI routes=%d operations=%d", len(routes), len(openAPI.OpenAPIOperations()))
	}
	actions, err := schedulersdk.SchedulerAuthorizationActions()
	if err != nil {
		t.Fatal(err)
	}
	provider := adapterProvider{value}
	if err := modulehttp.ValidateAuthorizationProjection(actions, provider); err != nil {
		t.Fatal(err)
	}
}

func TestAdapterReadsDefinitionsPreviewsAndState(t *testing.T) {
	binding := schedulerBindingFixture()
	value, err := NewAdapter(binding, binding)
	if err != nil {
		t.Fatal(err)
	}

	definitions := serve(value, http.MethodGet, "/scheduler/definitions", "", nil)
	if definitions.Code != http.StatusOK || !strings.Contains(definitions.Body.String(), `"key":"daily"`) || !strings.Contains(definitions.Body.String(), `"target_type":"workflow"`) {
		t.Fatalf("definitions=%d %s", definitions.Code, definitions.Body.String())
	}
	detail := serve(value, http.MethodGet, "/scheduler/definitions/daily", "", nil)
	if detail.Code != http.StatusOK || !strings.Contains(detail.Body.String(), `"schedule_type":"cron"`) {
		t.Fatalf("detail=%d %s", detail.Code, detail.Body.String())
	}
	missing := serve(value, http.MethodGet, "/scheduler/definitions/missing", "", nil)
	if missing.Code != http.StatusNotFound || !strings.Contains(missing.Body.String(), "backend.scheduler.definition_not_found") {
		t.Fatalf("missing=%d %s", missing.Code, missing.Body.String())
	}
	contract := serve(value, http.MethodGet, "/scheduler/authoring-contract", "", nil)
	if contract.Code != http.StatusOK || !strings.Contains(contract.Body.String(), `"mutation_owner":"source_controlled_json"`) {
		t.Fatalf("contract=%d %s", contract.Code, contract.Body.String())
	}
	preview := serve(value, http.MethodPost, "/scheduler/definitions/validate", `{"data":{"key":"daily","name":"Daily","status":"enabled","target_type":"workflow","target_key":"scheduled:daily","schedule_type":"cron","schedule_expression":"0 9 * * *","timezone":"Asia/Shanghai","max_attempts":1,"timeout_seconds":300}}`, nil)
	if preview.Code != http.StatusOK || !strings.Contains(preview.Body.String(), `"next_runs"`) {
		t.Fatalf("preview=%d %s", preview.Code, preview.Body.String())
	}
	invalid := serve(value, http.MethodPost, "/scheduler/schedules/preview", `{"schedule_type":"monthly_at","day_of_month":0}`, nil)
	if invalid.Code != http.StatusBadRequest || !strings.Contains(invalid.Body.String(), "backend.scheduler.time_of_day_invalid") {
		t.Fatalf("invalid=%d %s", invalid.Code, invalid.Body.String())
	}
	state := serve(value, http.MethodGet, "/scheduler/state", "", nil)
	if state.Code != http.StatusOK || !strings.Contains(state.Body.String(), `"id":"run-1"`) || !strings.Contains(state.Body.String(), `"id":"run-dead"`) {
		t.Fatalf("state=%d %s", state.Code, state.Body.String())
	}
}

func TestAdapterExecutesEverySchedulerCommandWithOperatorEvidence(t *testing.T) {
	binding := schedulerBindingFixture()
	value, err := NewAdapter(binding, binding)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		path string
		body string
		want string
	}{
		{"/scheduler/definitions/daily/run", "", `"id":"run-now"`},
		{"/scheduler/definitions/daily/reschedule", `{"next_run_at":"2026-09-06T09:00:00Z"}`, `"status":"rescheduled"`},
		{"/scheduler/runs/run-1/retry", "", `"status":"retrying"`},
		{"/scheduler/runs/run-1/cancel", "", `"status":"cancelled"`},
		{"/scheduler/dead-letters/run-dead/resolve", `{"note":"root cause corrected"}`, `"status":"resolved"`},
		{"/scheduler/dead-letters/run-dead/requeue", `{"note":"delivery restored"}`, `"status":"retrying"`},
	}
	for index, test := range tests {
		header := map[string]string{"X-Operation-Reason": "verified recovery", "Idempotency-Key": fmt.Sprintf("scheduler-command-%d", index)}
		response := serve(value, http.MethodPost, test.path, test.body, header)
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), test.want) {
			t.Errorf("%s=%d %s", test.path, response.Code, response.Body.String())
		}
	}
	if binding.commandResource != "run-dead" || binding.commandReason != "delivery restored" {
		t.Fatalf("last command resource=%q reason=%q", binding.commandResource, binding.commandReason)
	}
	if binding.rescheduledAt.Format(time.RFC3339) != "2026-09-06T09:00:00Z" {
		t.Fatalf("rescheduled=%s", binding.rescheduledAt)
	}
}

func TestAdapterDurablyReplaysSchedulerCommandsAndRejectsKeyReuse(t *testing.T) {
	binding := schedulerBindingFixture()
	value, err := NewAdapter(binding, binding)
	if err != nil {
		t.Fatal(err)
	}
	headers := map[string]string{"X-Operation-Reason": "operator requested", "Idempotency-Key": "manual-run-01"}
	first := serve(value, http.MethodPost, "/scheduler/definitions/daily/run", "", headers)
	replay := serve(value, http.MethodPost, "/scheduler/definitions/daily/run", "", headers)
	if first.Code != http.StatusOK || replay.Code != http.StatusOK || first.Body.String() != replay.Body.String() || replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("first=%d %s replay=%d %s headers=%v", first.Code, first.Body.String(), replay.Code, replay.Body.String(), replay.Header())
	}
	if binding.triggerCalls != 1 {
		t.Fatalf("trigger calls=%d want=1", binding.triggerCalls)
	}
	conflict := serve(value, http.MethodPost, "/scheduler/definitions/daily/run", "", map[string]string{"X-Operation-Reason": "different reason", "Idempotency-Key": "manual-run-01"})
	if conflict.Code != http.StatusConflict || !strings.Contains(conflict.Body.String(), "backend.scheduler.idempotency_conflict") {
		t.Fatalf("conflict=%d %s", conflict.Code, conflict.Body.String())
	}
	missing := serve(value, http.MethodPost, "/scheduler/definitions/daily/run", "", map[string]string{"X-Operation-Reason": "operator requested"})
	if missing.Code != http.StatusBadRequest || !strings.Contains(missing.Body.String(), "backend.scheduler.idempotency_key_required") {
		t.Fatalf("missing key=%d %s", missing.Code, missing.Body.String())
	}
}

func TestAdapterTreatsUnknownLengthEmptyBodyAsOrdinaryProductionRun(t *testing.T) {
	binding := schedulerBindingFixture()
	value, err := NewAdapter(binding, binding)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/scheduler/definitions/daily/run", strings.NewReader(""))
	request.ContentLength = -1
	request.Header.Set("X-Operation-Reason", "operator requested")
	request.Header.Set("Idempotency-Key", "manual-empty-body-01")
	response := httptest.NewRecorder()
	value.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || binding.triggerCalls != 1 {
		t.Fatalf("status=%d body=%s calls=%d", response.Code, response.Body.String(), binding.triggerCalls)
	}
}

func schedulerBindingFixture() *bindingStub {
	now := time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC)
	definition := schedulersdk.Definition{Key: "daily", Name: "Daily review", Status: "enabled", Revision: "v1", Schedule: schedulersdk.Schedule{Type: "cron", Expression: "0 9 * * *", Timezone: "Asia/Shanghai"}, Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "workflow", Operation: "scheduled:daily"}, Policy: schedulersdk.Policy{MaxAttempts: 3}}
	run := schedulersdk.Run{Trigger: schedulersdk.Trigger{RunID: "run-1", DefinitionKey: "daily", Attempt: 1, ScheduledFor: now}, Status: "failed", LastError: "timeout", CreatedAt: now, UpdatedAt: now}
	return &bindingStub{
		repository: &definitionRepositoryStub{snapshot: persistence.DefinitionSnapshot{Revision: 1, Definitions: []schedulersdk.Definition{definition}}},
		runs:       []schedulersdk.Run{run}, deadLetters: []schedulersdk.DeadLetter{{RunID: "run-dead", DefinitionKey: "daily", Status: "open", FailedAt: now}},
		triggeredRun:    schedulersdk.Run{Trigger: schedulersdk.Trigger{RunID: "run-now", DefinitionKey: "daily"}, Status: "succeeded"},
		retriedRun:      schedulersdk.Run{Trigger: schedulersdk.Trigger{RunID: "run-1", DefinitionKey: "daily"}, Status: "retrying"},
		cancelledRun:    schedulersdk.Run{Trigger: schedulersdk.Trigger{RunID: "run-1", DefinitionKey: "daily"}, Status: "cancelled"},
		resolvedLetter:  schedulersdk.DeadLetter{RunID: "run-dead", DefinitionKey: "daily", Status: "resolved"},
		requeuedRun:     schedulersdk.Run{Trigger: schedulersdk.Trigger{RunID: "run-dead", DefinitionKey: "daily"}, Status: "retrying"},
		commandReceipts: map[string]schedulermodel.CommandReceipt{},
	}
}

func serve(value modulehttp.Adapter, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response := httptest.NewRecorder()
	value.Handler().ServeHTTP(response, request)
	return response
}

type adapterProvider struct{ modulehttp.Adapter }

func (p adapterProvider) HTTPAdapters() []modulehttp.Adapter { return []modulehttp.Adapter{p.Adapter} }

func decodeBody(t *testing.T, response *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	value := map[string]any{}
	if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	return value
}
