package saas

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulerhttptransport "github.com/domainry/domainry-scheduler-sdk/saashost/httptransport"
	schedulercapability "github.com/domainry/domainry-scheduler/capability"
	schedulerstore "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence/database"
	schedulermigration "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence/database/migration"
	saashttp "github.com/domainry/domainry-scheduler/internal/transport/http/saas"
	_ "modernc.org/sqlite"
)

type databaseDownstream struct{}

type databaseHTTPRoundTripper struct{ handler http.Handler }

func (transport databaseHTTPRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	response := httptest.NewRecorder()
	transport.handler.ServeHTTP(response, request)
	return response.Result(), nil
}

type recordingDatabaseDownstream struct {
	mu       sync.Mutex
	triggers []schedulersdk.Trigger
	dispatch chan schedulersdk.Trigger
}

type failingPlanDownstream struct {
	calls chan schedulersdk.Trigger
}

func (d *failingPlanDownstream) Dispatch(_ context.Context, trigger schedulersdk.Trigger) (schedulersdk.DownstreamReceipt, error) {
	d.calls <- trigger
	return schedulersdk.DownstreamReceipt{}, errors.New("temporary downstream failure")
}

func (*failingPlanDownstream) ResolveHTTPConnection(context.Context, string) (modulehost.HTTPConnection, error) {
	return modulehost.HTTPConnection{}, nil
}

func (databaseDownstream) Dispatch(_ context.Context, trigger schedulersdk.Trigger) (schedulersdk.DownstreamReceipt, error) {
	return schedulersdk.DownstreamReceipt{ID: "receipt-" + trigger.RunID, Owner: trigger.Target.Owner, Status: "accepted"}, nil
}

func (d *recordingDatabaseDownstream) Dispatch(_ context.Context, trigger schedulersdk.Trigger) (schedulersdk.DownstreamReceipt, error) {
	d.mu.Lock()
	d.triggers = append(d.triggers, trigger)
	d.mu.Unlock()
	if d.dispatch != nil {
		select {
		case d.dispatch <- trigger:
		default:
		}
	}
	return schedulersdk.DownstreamReceipt{ID: "receipt-" + trigger.RunID, Owner: trigger.Target.Owner, Status: "accepted"}, nil
}

func (*recordingDatabaseDownstream) ResolveHTTPConnection(context.Context, string) (modulehost.HTTPConnection, error) {
	return modulehost.HTTPConnection{}, nil
}

func TestDatabaseServiceLifetimeDoesNotFollowOpeningRequest(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-saas-lifetime?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, err := NewDatabaseService(DatabaseServiceOptions{
		Context: t.Context(), Database: db, Driver: "sqlite", WorkerID: "scheduler-saas-lifetime",
		Worker: schedulersdk.WorkerConfig{Enabled: true, PollInterval: time.Hour, BatchSize: 1},
		Downstreams: func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error) {
			return databaseDownstream{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	requestCtx, cancelRequest := context.WithCancel(t.Context())
	ref := schedulersdk.ApplicationRef{RuntimeID: "runtime-lifetime"}
	session := beginDatabasePublication(t, service, ref)
	definition := schedulersdk.Definition{Key: "daily", Status: "enabled", Revision: "v1", Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60}, Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "workflow", Operation: "run"}}
	if err := service.Reconcile(requestCtx, ref, schedulersdk.DefinitionSnapshot{PublisherSession: &session, Revision: 1, Definitions: []schedulersdk.Definition{definition}}); err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	done := service.applications[ref.RuntimeID].done
	service.mu.Unlock()
	cancelRequest()
	select {
	case <-done:
		t.Fatal("Scheduler worker inherited the opening request lifetime")
	case <-time.After(20 * time.Millisecond):
	}
	if err := service.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Scheduler worker did not stop during service shutdown")
	}
}

func TestDatabaseServicePersistsScheduledPlansAcrossOwnerRestart(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-saas-plans?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ref := schedulersdk.ApplicationRef{RuntimeID: "runtime-plans"}
	open := func(workerID string) *DatabaseService {
		service, openErr := NewDatabaseService(DatabaseServiceOptions{
			Context: t.Context(), Database: db, Driver: "sqlite", WorkerID: workerID, Worker: schedulersdk.WorkerConfig{Enabled: false}, Applications: []schedulersdk.ApplicationRef{ref},
			Downstreams: func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error) {
				return databaseDownstream{}, nil
			},
		})
		if openErr != nil {
			t.Fatal(openErr)
		}
		return service
	}
	owner := schedulersdk.ScheduledPlanOwner{WorkspaceID: "workspace-a", UserID: "user-a", ProductKey: "agent"}
	first := open("scheduler-plan-a")
	receipt, err := first.CreateScheduledPlan(t.Context(), ref, schedulersdk.ScheduledPlanCreate{
		ClientID: "weekly", Name: "每周整理待办", Owner: owner, Timezone: "Asia/Shanghai",
		Trigger: schedulersdk.ScheduledPlanTrigger{Type: "recurring", Schedule: &schedulersdk.Schedule{Type: "weekly_at", TimeOfDay: "09:00", DayOfWeek: "monday", Timezone: "Asia/Shanghai"}},
		Input:   json.RawMessage(`{"goal":"整理未完成待办"}`), AllowedActions: []string{"todo.list", "artifact.create"},
		Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "agent", Operation: "conversation_task_start"}, ConversationRef: schedulersdk.ScheduledPlanConversationRef{ConversationID: "conversation-a"},
	})
	if err != nil || receipt.Replay || receipt.Plan.ID == "" {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
	if err := first.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	second := open("scheduler-plan-b")
	defer second.Shutdown(t.Context())
	loaded, err := second.GetScheduledPlan(t.Context(), ref, schedulersdk.ScheduledPlanLookup{Owner: owner, PlanID: receipt.Plan.ID})
	if err != nil || loaded.ID != receipt.Plan.ID || loaded.Trigger.Schedule == nil || loaded.Trigger.Schedule.DayOfWeek != "monday" || loaded.ConversationRef.ConversationID != "conversation-a" {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	other := owner
	other.WorkspaceID = "workspace-b"
	if _, err := second.GetScheduledPlan(t.Context(), ref, schedulersdk.ScheduledPlanLookup{Owner: other, PlanID: receipt.Plan.ID}); !errors.Is(err, schedulersdk.ErrScheduledPlanNotFound) {
		t.Fatalf("cross-workspace read err=%v", err)
	}
	otherRuntime := schedulersdk.ApplicationRef{RuntimeID: "runtime-other"}
	if _, err := second.GetScheduledPlan(t.Context(), otherRuntime, schedulersdk.ScheduledPlanLookup{Owner: owner, PlanID: receipt.Plan.ID}); err == nil {
		t.Fatal("configured SaaS plan service accepted another Runtime")
	}
}

func TestScheduledPlanWorkerRetriesOnceAfterSaaSRestartWithoutDuplicateWindow(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-plan-retry-restart?mode=memory&cache=shared&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ref := schedulersdk.ApplicationRef{RuntimeID: "runtime-plan-retry"}
	failedCalls := make(chan schedulersdk.Trigger, 2)
	firstDownstream := &failingPlanDownstream{calls: failedCalls}
	first, err := NewDatabaseService(DatabaseServiceOptions{
		Context: t.Context(), Database: db, Driver: "sqlite", WorkerID: "worker-before-restart",
		Worker:       schedulersdk.WorkerConfig{Enabled: true, PollInterval: 5 * time.Millisecond, BatchSize: 5, LeaseTTL: time.Second},
		Applications: []schedulersdk.ApplicationRef{ref},
		Downstreams: func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error) {
			return firstDownstream, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	firstServer, err := saashttp.New(saashttp.Options{ApplicationTokens: map[string]string{ref.RuntimeID: "scheduler-token"}, Service: first})
	if err != nil {
		t.Fatal(err)
	}
	capabilityBinding, err := schedulercapability.Open(schedulercapability.Inputs{})
	if err != nil {
		t.Fatal(err)
	}
	capabilitySummary, err := capabilityBinding.CapabilitySummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	clientTransport, err := schedulerhttptransport.Open(t.Context(), schedulerhttptransport.Config{
		Endpoint: "http://scheduler.test", Token: "scheduler-token",
		Client: &http.Client{Transport: databaseHTTPRoundTripper{handler: firstServer}}, CapabilityContractSHA256: capabilitySummary.Identity.ContractSHA256,
	})
	if err != nil {
		t.Fatal(err)
	}
	dueAt := time.Now().UTC().Add(30 * time.Millisecond)
	owner := schedulersdk.ScheduledPlanOwner{WorkspaceID: "workspace-a", UserID: "user-a", ProductKey: "agent"}
	receipt, err := clientTransport.CreateScheduledPlan(t.Context(), ref, schedulersdk.ScheduledPlanCreate{
		ClientID: "restart-once", Name: "服务重启后继续", Owner: owner, Timezone: "Asia/Shanghai",
		Trigger: schedulersdk.ScheduledPlanTrigger{Type: schedulersdk.ScheduledPlanTriggerOnce, At: &dueAt, Policy: schedulersdk.Policy{
			Misfire: schedulersdk.ScheduledPlanMisfireCatchOne, MisfireGrace: time.Minute, MaxAttempts: 3,
			RetryInitial: 200 * time.Millisecond, RetryMax: time.Second, Timeout: time.Second,
		}},
		Input: json.RawMessage(`{"goal":"整理未完成待办"}`), AllowedActions: []string{"todo.list", "artifact.create"},
		Target:          schedulersdk.TargetRef{Type: "runtime_operation", Owner: "agent", Operation: "conversation_task_start"},
		ConversationRef: schedulersdk.ScheduledPlanConversationRef{ConversationID: "conversation-a", RunID: "run-a"},
	})
	if err != nil || receipt.Plan.ID == "" {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
	var firstTrigger schedulersdk.Trigger
	select {
	case firstTrigger = <-failedCalls:
	case <-time.After(2 * time.Second):
		t.Fatal("first plan attempt did not dispatch")
	}
	waitSchedulerRunStatus(t, db, ref.RuntimeID, firstTrigger.RunID, "retrying")
	if err := first.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}

	succeededCalls := make(chan schedulersdk.Trigger, 2)
	secondDownstream := &recordingDatabaseDownstream{dispatch: succeededCalls}
	second, err := NewDatabaseService(DatabaseServiceOptions{
		Context: t.Context(), Database: db, Driver: "sqlite", WorkerID: "worker-after-restart",
		Worker:       schedulersdk.WorkerConfig{Enabled: true, PollInterval: 5 * time.Millisecond, BatchSize: 5, LeaseTTL: time.Second},
		Applications: []schedulersdk.ApplicationRef{ref},
		Downstreams: func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error) {
			return secondDownstream, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Shutdown(t.Context())
	var recovered schedulersdk.Trigger
	select {
	case recovered = <-succeededCalls:
	case <-time.After(2 * time.Second):
		t.Fatal("persisted plan retry did not resume after Scheduler restart")
	}
	if recovered.RunID != firstTrigger.RunID || recovered.IdempotencyKey != firstTrigger.IdempotencyKey || recovered.Attempt != 2 {
		t.Fatalf("first=%+v recovered=%+v", firstTrigger, recovered)
	}
	var payload schedulersdk.ScheduledPlanDispatch
	if err := json.Unmarshal(recovered.Target.Payload, &payload); err != nil || payload.ContractVersion != schedulersdk.ScheduledPlanDispatchContractVersion || payload.PlanID != receipt.Plan.ID || payload.Owner != owner || payload.ConversationRef.RunID != "run-a" || len(payload.AllowedActions) != 2 || string(payload.Input) != `{"goal":"整理未完成待办"}` {
		t.Fatalf("payload=%+v raw=%s err=%v", payload, recovered.Target.Payload, err)
	}
	waitSchedulerRunStatus(t, db, ref.RuntimeID, recovered.RunID, "succeeded")
	var runs, retries int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _scheduler_runs WHERE runtime_id = ? AND definition_key = ?`, ref.RuntimeID, "scheduled-plan:"+receipt.Plan.ID).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _scheduler_run_events WHERE runtime_id = ? AND run_id = ? AND event_type = 'retry_scheduled'`, ref.RuntimeID, recovered.RunID).Scan(&retries); err != nil {
		t.Fatal(err)
	}
	var attempt int
	var fencing int64
	if err := db.QueryRowContext(t.Context(), `SELECT attempt, fencing_token FROM _scheduler_runs WHERE runtime_id = ? AND run_id = ?`, ref.RuntimeID, recovered.RunID).Scan(&attempt, &fencing); err != nil {
		t.Fatal(err)
	}
	if runs != 1 || retries != 1 || attempt != 2 || fencing != 2 {
		t.Fatalf("runs=%d retries=%d attempt=%d fencing=%d", runs, retries, attempt, fencing)
	}
	select {
	case duplicate := <-succeededCalls:
		t.Fatalf("one-time window dispatched twice after success: %+v", duplicate)
	case <-time.After(50 * time.Millisecond):
	}
}

func waitSchedulerRunStatus(t *testing.T, db *sql.DB, runtimeID, runID, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		if err := db.QueryRowContext(t.Context(), `SELECT status FROM _scheduler_runs WHERE runtime_id = ? AND run_id = ?`, runtimeID, runID).Scan(&status); err == nil && status == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("run %s did not reach %s", runID, want)
}

func TestScheduledPlanSaaSHTTPPersistsExactCommandAndOwnerScope(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-saas-plan-http?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ref := schedulersdk.ApplicationRef{RuntimeID: "runtime-plan-http"}
	openService := func(workerID string) *DatabaseService {
		service, openErr := NewDatabaseService(DatabaseServiceOptions{
			Context: t.Context(), Database: db, Driver: "sqlite", WorkerID: workerID, Worker: schedulersdk.WorkerConfig{Enabled: false}, Applications: []schedulersdk.ApplicationRef{ref},
			Downstreams: func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error) {
				return databaseDownstream{}, nil
			},
		})
		if openErr != nil {
			t.Fatal(openErr)
		}
		return service
	}
	request := schedulersdk.ScheduledPlanCreate{
		ClientID: "weekly-http", Name: "每周一整理待办", Owner: schedulersdk.ScheduledPlanOwner{WorkspaceID: "workspace-a", UserID: "user-a", ProductKey: "agent"}, Timezone: "Asia/Shanghai",
		Trigger: schedulersdk.ScheduledPlanTrigger{Type: "recurring", Schedule: &schedulersdk.Schedule{Type: "weekly_at", TimeOfDay: "09:00", DayOfWeek: "monday", Timezone: "Asia/Shanghai"}},
		Input:   json.RawMessage(`{"goal":"整理本周待办","include_status":"open"}`), AllowedActions: []string{"todo.list", "artifact.create"},
		Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "agent", Operation: "conversation_task_start"}, ConversationRef: schedulersdk.ScheduledPlanConversationRef{ConversationID: "conversation-a", RunID: "run-a"},
	}
	call := func(handler http.Handler, method, path string, body any) *httptest.ResponseRecorder {
		raw := []byte{}
		if body != nil {
			var marshalErr error
			raw, marshalErr = json.Marshal(body)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
		}
		httpRequest := httptest.NewRequest(method, path, bytes.NewReader(raw))
		httpRequest.Header.Set("Authorization", "Bearer scheduler-token")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httpRequest)
		return response
	}
	first := openService("scheduler-plan-http-a")
	server, err := saashttp.New(saashttp.Options{ApplicationTokens: map[string]string{ref.RuntimeID: "scheduler-token"}, Service: first})
	if err != nil {
		t.Fatal(err)
	}
	capabilityBinding, err := schedulercapability.Open(schedulercapability.Inputs{})
	if err != nil {
		t.Fatal(err)
	}
	capabilitySummary, err := capabilityBinding.CapabilitySummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	clientTransport, err := schedulerhttptransport.Open(t.Context(), schedulerhttptransport.Config{Endpoint: "http://scheduler.test", Token: "scheduler-token", Client: &http.Client{Transport: databaseHTTPRoundTripper{handler: server}}, CapabilityContractSHA256: capabilitySummary.Identity.ContractSHA256})
	if err != nil {
		t.Fatal(err)
	}
	sdkRequest := request
	sdkRequest.ClientID = "weekly-sdk"
	sdkReceipt, err := clientTransport.CreateScheduledPlan(t.Context(), ref, sdkRequest)
	if err != nil || sdkReceipt.Replay || sdkReceipt.Plan.ID == "" {
		t.Fatalf("SDK create receipt=%+v err=%v", sdkReceipt, err)
	}
	response := call(server, http.MethodPost, "/v1/applications/"+ref.RuntimeID+"/plans", request)
	if response.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
	}
	var receipt schedulersdk.ScheduledPlanReceipt
	if err := json.Unmarshal(response.Body.Bytes(), &receipt); err != nil || receipt.Replay || receipt.Plan.ID == "" {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
	replay := call(server, http.MethodPost, "/v1/applications/"+ref.RuntimeID+"/plans", request)
	var replayReceipt schedulersdk.ScheduledPlanReceipt
	if err := json.Unmarshal(replay.Body.Bytes(), &replayReceipt); err != nil || replay.Code != http.StatusOK || !replayReceipt.Replay || replayReceipt.Plan.ID != receipt.Plan.ID {
		t.Fatalf("replay status=%d receipt=%+v err=%v", replay.Code, replayReceipt, err)
	}
	changed := request
	changed.Name = "same key, changed content"
	conflict := call(server, http.MethodPost, "/v1/applications/"+ref.RuntimeID+"/plans", changed)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	if err := first.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	second := openService("scheduler-plan-http-b")
	restarted, err := saashttp.New(saashttp.Options{ApplicationTokens: map[string]string{ref.RuntimeID: "scheduler-token"}, Service: second})
	if err != nil {
		t.Fatal(err)
	}
	restartedTransport, err := schedulerhttptransport.Open(t.Context(), schedulerhttptransport.Config{Endpoint: "http://scheduler.test", Token: "scheduler-token", Client: &http.Client{Transport: databaseHTTPRoundTripper{handler: restarted}}, CapabilityContractSHA256: capabilitySummary.Identity.ContractSHA256})
	if err != nil {
		t.Fatal(err)
	}
	sdkLoaded, err := restartedTransport.GetScheduledPlan(t.Context(), ref, schedulersdk.ScheduledPlanLookup{Owner: request.Owner, PlanID: sdkReceipt.Plan.ID})
	if err != nil || sdkLoaded.ID != sdkReceipt.Plan.ID || sdkLoaded.ConversationRef.RunID != "run-a" {
		t.Fatalf("SDK loaded=%+v err=%v", sdkLoaded, err)
	}
	ownerQuery := "?workspace_id=workspace-a&user_id=user-a&product_key=agent"
	loadedResponse := call(restarted, http.MethodGet, "/v1/applications/"+ref.RuntimeID+"/plans/"+receipt.Plan.ID+ownerQuery, nil)
	var loaded schedulersdk.ScheduledPlan
	if err := json.Unmarshal(loadedResponse.Body.Bytes(), &loaded); err != nil || loadedResponse.Code != http.StatusOK || loaded.ID != receipt.Plan.ID || loaded.Trigger.Schedule == nil || loaded.Trigger.Schedule.DayOfWeek != "monday" || loaded.ConversationRef.RunID != "run-a" {
		t.Fatalf("loaded status=%d plan=%+v err=%v", loadedResponse.Code, loaded, err)
	}
	forbidden := call(restarted, http.MethodGet, "/v1/applications/"+ref.RuntimeID+"/plans/"+receipt.Plan.ID+"?workspace_id=workspace-a&user_id=user-b&product_key=agent", nil)
	if forbidden.Code != http.StatusNotFound {
		t.Fatalf("cross-user status=%d body=%s", forbidden.Code, forbidden.Body.String())
	}
	page, err := restartedTransport.ListScheduledPlans(t.Context(), ref, schedulersdk.ScheduledPlanList{Owner: request.Owner, Limit: 10})
	if err != nil || len(page.Items) != 2 {
		t.Fatalf("management list=%+v err=%v", page, err)
	}
	updated, err := restartedTransport.UpdateScheduledPlan(t.Context(), ref, schedulersdk.ScheduledPlanUpdate{
		Owner: request.Owner, PlanID: sdkReceipt.Plan.ID, ExpectedRevision: 1, Name: "每周二整理待办", Timezone: "Asia/Shanghai",
		Trigger: schedulersdk.ScheduledPlanTrigger{Type: "recurring", Schedule: &schedulersdk.Schedule{Type: "weekly_at", TimeOfDay: "10:30", DayOfWeek: "tuesday", Timezone: "Asia/Shanghai"}},
		Input:   sdkReceipt.Plan.Input, AllowedActions: sdkReceipt.Plan.AllowedActions, Target: sdkReceipt.Plan.Target, ConversationRef: sdkReceipt.Plan.ConversationRef,
	})
	if err != nil || updated.Plan.Revision != 2 || updated.Plan.Trigger.Schedule == nil || updated.Plan.Trigger.Schedule.DayOfWeek != "tuesday" {
		t.Fatalf("management update=%+v err=%v", updated, err)
	}
	if _, err := restartedTransport.UpdateScheduledPlan(t.Context(), ref, schedulersdk.ScheduledPlanUpdate{Owner: request.Owner, PlanID: sdkReceipt.Plan.ID, ExpectedRevision: 1, Name: "stale", Timezone: updated.Plan.Timezone, Trigger: updated.Plan.Trigger, Input: updated.Plan.Input, AllowedActions: updated.Plan.AllowedActions, Target: updated.Plan.Target, ConversationRef: updated.Plan.ConversationRef}); !errors.Is(err, schedulersdk.ErrScheduledPlanConflict) {
		t.Fatalf("stale SDK update err=%v", err)
	}
	paused, err := restartedTransport.PauseScheduledPlan(t.Context(), ref, schedulersdk.ScheduledPlanStatusChange{Owner: request.Owner, PlanID: sdkReceipt.Plan.ID, ExpectedRevision: 2})
	if err != nil || paused.Plan.Status != schedulersdk.ScheduledPlanStatusPaused || paused.Plan.Revision != 3 {
		t.Fatalf("management pause=%+v err=%v", paused, err)
	}
	pauseReplay, err := restartedTransport.PauseScheduledPlan(t.Context(), ref, schedulersdk.ScheduledPlanStatusChange{Owner: request.Owner, PlanID: sdkReceipt.Plan.ID, ExpectedRevision: 2})
	if err != nil || !pauseReplay.Replay || pauseReplay.Plan.Revision != 3 {
		t.Fatalf("management pause replay=%+v err=%v", pauseReplay, err)
	}
	resumed, err := restartedTransport.ResumeScheduledPlan(t.Context(), ref, schedulersdk.ScheduledPlanStatusChange{Owner: request.Owner, PlanID: sdkReceipt.Plan.ID, ExpectedRevision: 3})
	if err != nil || resumed.Plan.Status != schedulersdk.ScheduledPlanStatusEnabled || resumed.Plan.Revision != 4 {
		t.Fatalf("management resume=%+v err=%v", resumed, err)
	}
	deleted, err := restartedTransport.DeleteScheduledPlan(t.Context(), ref, schedulersdk.ScheduledPlanStatusChange{Owner: request.Owner, PlanID: sdkReceipt.Plan.ID, ExpectedRevision: 4})
	if err != nil || !deleted.Deleted || deleted.Revision != 5 {
		t.Fatalf("management delete=%+v err=%v", deleted, err)
	}
	deleteReplay, err := restartedTransport.DeleteScheduledPlan(t.Context(), ref, schedulersdk.ScheduledPlanStatusChange{Owner: request.Owner, PlanID: sdkReceipt.Plan.ID, ExpectedRevision: 4})
	if err != nil || !deleteReplay.Replay || deleteReplay.Revision != 5 {
		t.Fatalf("management delete replay=%+v err=%v", deleteReplay, err)
	}
	if _, err := restartedTransport.GetScheduledPlan(t.Context(), ref, schedulersdk.ScheduledPlanLookup{Owner: request.Owner, PlanID: sdkReceipt.Plan.ID}); !errors.Is(err, schedulersdk.ErrScheduledPlanNotFound) {
		t.Fatalf("deleted SDK get err=%v", err)
	}
	otherOwner := request.Owner
	otherOwner.UserID = "user-b"
	if _, err := restartedTransport.PauseScheduledPlan(t.Context(), ref, schedulersdk.ScheduledPlanStatusChange{Owner: otherOwner, PlanID: receipt.Plan.ID, ExpectedRevision: 1}); !errors.Is(err, schedulersdk.ErrScheduledPlanNotFound) {
		t.Fatalf("cross-user SDK pause err=%v", err)
	}
	if err := second.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	third := openService("scheduler-plan-http-c")
	defer third.Shutdown(t.Context())
	thirdServer, err := saashttp.New(saashttp.Options{ApplicationTokens: map[string]string{ref.RuntimeID: "scheduler-token"}, Service: third})
	if err != nil {
		t.Fatal(err)
	}
	thirdTransport, err := schedulerhttptransport.Open(t.Context(), schedulerhttptransport.Config{Endpoint: "http://scheduler.test", Token: "scheduler-token", Client: &http.Client{Transport: databaseHTTPRoundTripper{handler: thirdServer}}, CapabilityContractSHA256: capabilitySummary.Identity.ContractSHA256})
	if err != nil {
		t.Fatal(err)
	}
	page, err = thirdTransport.ListScheduledPlans(t.Context(), ref, schedulersdk.ScheduledPlanList{Owner: request.Owner, Limit: 10})
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != receipt.Plan.ID {
		t.Fatalf("post-delete restart page=%+v err=%v", page, err)
	}
	if _, err := thirdTransport.GetScheduledPlan(t.Context(), ref, schedulersdk.ScheduledPlanLookup{Owner: request.Owner, PlanID: sdkReceipt.Plan.ID}); !errors.Is(err, schedulersdk.ErrScheduledPlanNotFound) {
		t.Fatalf("tombstone revived after restart err=%v", err)
	}
}

func TestDatabaseServiceRestartReadDoesNotDisablePersistedDefinitions(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-saas-restart-definitions?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	newService := func(workerID string) *DatabaseService {
		service, openErr := NewDatabaseService(DatabaseServiceOptions{
			Context: t.Context(), Database: db, Driver: "sqlite", WorkerID: workerID,
			Worker: schedulersdk.WorkerConfig{Enabled: true, PollInterval: time.Hour, BatchSize: 1},
			Downstreams: func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error) {
				return databaseDownstream{}, nil
			},
		})
		if openErr != nil {
			t.Fatal(openErr)
		}
		return service
	}
	ref := schedulersdk.ApplicationRef{RuntimeID: "runtime-restart"}
	otherRef := schedulersdk.ApplicationRef{RuntimeID: "runtime-restart-other"}
	first := newService("scheduler-restart-a")
	session := beginDatabasePublication(t, first, ref)
	otherSession := beginDatabasePublication(t, first, otherRef)
	snapshot := schedulersdk.DefinitionSnapshot{PublisherSession: &session, Revision: 7, Definitions: []schedulersdk.Definition{testDatabaseDefinition("persisted", "run-persisted")}}
	otherSnapshot := schedulersdk.DefinitionSnapshot{PublisherSession: &otherSession, Revision: 5, Definitions: []schedulersdk.Definition{testDatabaseDefinition("other", "run-other")}}
	if err := first.Reconcile(t.Context(), ref, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := first.Reconcile(t.Context(), otherRef, otherSnapshot); err != nil {
		t.Fatal(err)
	}
	firstRun, err := first.TriggerNow(t.Context(), ref, "persisted", "before restart")
	if err != nil {
		t.Fatal(err)
	}
	otherRun, err := first.TriggerNow(t.Context(), otherRef, "other", "before restart")
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}

	second := newService("scheduler-restart-b")
	for _, check := range []struct {
		ref   schedulersdk.ApplicationRef
		runID string
	}{{ref: ref, runID: firstRun.Trigger.RunID}, {ref: otherRef, runID: otherRun.Trigger.RunID}} {
		runs, listErr := second.Runs(t.Context(), check.ref, 10)
		if listErr != nil || len(runs) != 1 || runs[0].Trigger.RunID != check.runID {
			t.Fatalf("runtime=%s runs=%+v err=%v", check.ref.RuntimeID, runs, listErr)
		}
		got, getErr := second.Run(t.Context(), check.ref, check.runID)
		if getErr != nil || got.Trigger.RunID != check.runID {
			t.Fatalf("runtime=%s run=%+v err=%v", check.ref.RuntimeID, got, getErr)
		}
	}
	if _, err := second.Run(t.Context(), otherRef, firstRun.Trigger.RunID); err == nil {
		t.Fatal("runtime-restart-other read runtime-restart run after restart")
	}
	second.mu.Lock()
	opened := second.applications[ref.RuntimeID]
	second.mu.Unlock()
	opened.mu.RLock()
	startedBeforeSnapshot := opened.cancel != nil || opened.done != nil
	opened.mu.RUnlock()
	if !startedBeforeSnapshot {
		t.Fatal("restart did not hydrate durable definitions and resume the SaaS worker")
	}
	assertActiveDefinitions(t, db, ref.RuntimeID, 1)
	if _, err := second.TriggerNow(t.Context(), ref, "persisted", "after Scheduler-only restart"); err != nil {
		t.Fatal(err)
	}
	if _, err := second.TriggerNow(t.Context(), otherRef, "persisted", "cross-tenant hydrated probe"); err == nil {
		t.Fatal("other Runtime executed a cross-tenant hydrated definition")
	}
	// No Runtime request is required: the hydrated snapshot remains active
	// after a Scheduler-only restart. Cross-restart publisher fencing awaits the
	// SDK session-generation contract and is not inferred from bare Revision.
	opened.mu.RLock()
	startedAfterHydrate := opened.cancel != nil && opened.done != nil
	opened.mu.RUnlock()
	if !startedAfterHydrate {
		t.Fatal("durable hydration did not keep the SaaS worker active")
	}
	assertActiveDefinitions(t, db, ref.RuntimeID, 1)
	if _, err := second.TriggerNow(t.Context(), ref, "persisted", "after Runtime reconnect"); err != nil {
		t.Fatal(err)
	}
	if _, err := second.TriggerNow(t.Context(), otherRef, "persisted", "cross-tenant after reconnect"); err == nil {
		t.Fatal("other Runtime executed a cross-tenant persisted definition after reconnect")
	}
	if err := second.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseServiceSerializesConcurrentFirstRuntimeSnapshots(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-saas-concurrent-first-reconcile?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, err := NewDatabaseService(DatabaseServiceOptions{
		Context: t.Context(), Database: db, Driver: "sqlite", WorkerID: "scheduler-concurrent-first",
		Worker: schedulersdk.WorkerConfig{Enabled: true, PollInterval: time.Hour, BatchSize: 1},
		Downstreams: func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error) {
			return databaseDownstream{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Shutdown(t.Context())
	ref := schedulersdk.ApplicationRef{RuntimeID: "runtime-concurrent"}
	session := beginDatabasePublication(t, service, ref)
	// A pre-Reconcile read creates the application but owns no configuration
	// publication and therefore must not start an empty-snapshot worker.
	if _, err := service.Runs(t.Context(), ref, 1); err != nil {
		t.Fatal(err)
	}
	snapshot := schedulersdk.DefinitionSnapshot{PublisherSession: &session, Revision: 7, Definitions: []schedulersdk.Definition{
		testDatabaseDefinition("first", "run-first"), testDatabaseDefinition("second", "run-second"),
	}}
	start := make(chan struct{})
	errorsByCall := make(chan error, 8)
	staleSnapshot := schedulersdk.DefinitionSnapshot{PublisherSession: &session, Revision: 6, Definitions: []schedulersdk.Definition{testDatabaseDefinition("first", "stale-first")}}
	var calls sync.WaitGroup
	for index := 0; index < cap(errorsByCall); index++ {
		candidate := snapshot
		if index%2 == 0 {
			candidate = staleSnapshot
		}
		calls.Add(1)
		go func(value schedulersdk.DefinitionSnapshot) {
			defer calls.Done()
			<-start
			errorsByCall <- service.Reconcile(t.Context(), ref, value)
		}(candidate)
	}
	close(start)
	calls.Wait()
	close(errorsByCall)
	for reconcileErr := range errorsByCall {
		if reconcileErr != nil && !errors.Is(reconcileErr, ErrStaleDefinitionSnapshot) {
			t.Fatal(reconcileErr)
		}
	}
	assertActiveDefinitions(t, db, ref.RuntimeID, 2)
	if err := service.Reconcile(t.Context(), ref, staleSnapshot); !errors.Is(err, ErrStaleDefinitionSnapshot) {
		t.Fatalf("late revision 6 after revision 7 err=%v", err)
	}
	if _, err := service.TriggerNow(t.Context(), ref, "first", "concurrent first snapshot"); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseServiceSchedulerOnlyRestartHydratesAndResumesDueTick(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-saas-restart-resume-tick?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	refA := schedulersdk.ApplicationRef{RuntimeID: "runtime-timer-a"}
	refB := schedulersdk.ApplicationRef{RuntimeID: "runtime-timer-b"}
	due := testDatabaseDefinition("due", "run-a")
	due.InitialNextRunAt = time.Now().UTC().Add(-time.Minute)
	future := testDatabaseDefinition("future", "run-b")
	future.InitialNextRunAt = time.Now().UTC().Add(time.Hour)
	first, err := NewDatabaseService(DatabaseServiceOptions{
		Context: t.Context(), Database: db, Driver: "sqlite", WorkerID: "scheduler-before-rolling-restart",
		Worker: schedulersdk.WorkerConfig{Enabled: false, PollInterval: time.Hour, BatchSize: 1},
		Downstreams: func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error) {
			return databaseDownstream{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	sessionA := beginDatabasePublication(t, first, refA)
	sessionB := beginDatabasePublication(t, first, refB)
	if err := first.Reconcile(t.Context(), refA, schedulersdk.DefinitionSnapshot{PublisherSession: &sessionA, Revision: 4, Definitions: []schedulersdk.Definition{due}}); err != nil {
		t.Fatal(err)
	}
	if err := first.Reconcile(t.Context(), refB, schedulersdk.DefinitionSnapshot{PublisherSession: &sessionB, Revision: 3, Definitions: []schedulersdk.Definition{future}}); err != nil {
		t.Fatal(err)
	}
	if err := first.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}

	dispatchedA := make(chan schedulersdk.Trigger, 1)
	dispatchedB := make(chan schedulersdk.Trigger, 1)
	downstreams := map[string]*recordingDatabaseDownstream{
		refA.RuntimeID: {dispatch: dispatchedA}, refB.RuntimeID: {dispatch: dispatchedB},
	}
	second, err := NewDatabaseService(DatabaseServiceOptions{
		Context: t.Context(), Database: db, Driver: "sqlite", WorkerID: "scheduler-after-rolling-restart",
		Worker:       schedulersdk.WorkerConfig{Enabled: true, PollInterval: time.Millisecond, BatchSize: 10},
		Applications: []schedulersdk.ApplicationRef{refA, refB},
		Downstreams: func(_ context.Context, ref schedulersdk.ApplicationRef) (DownstreamHost, error) {
			return downstreams[ref.RuntimeID], nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Shutdown(t.Context())
	// Configured applications are opened during service construction. Durable
	// hydration resumes the worker without waiting for any business request.
	select {
	case trigger := <-dispatchedA:
		if trigger.DefinitionKey != "due" || trigger.Target.Operation != "run-a" {
			t.Fatalf("runtime-a resumed wrong trigger=%+v", trigger)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Scheduler-only restart did not resume the due timer")
	}
	select {
	case trigger := <-dispatchedB:
		t.Fatalf("runtime-a hydration dispatched runtime-b trigger=%+v", trigger)
	default:
	}
}

func TestDatabaseServiceConfiguredApplicationsAreAClosedTenantSet(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-saas-configured-applications?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ref := schedulersdk.ApplicationRef{RuntimeID: "runtime-configured"}
	service, err := NewDatabaseService(DatabaseServiceOptions{
		Context: t.Context(), Database: db, Driver: "sqlite", WorkerID: "scheduler-configured-applications",
		Worker:       schedulersdk.WorkerConfig{Enabled: false},
		Applications: []schedulersdk.ApplicationRef{ref},
		Downstreams: func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error) {
			return databaseDownstream{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Shutdown(t.Context())
	if _, err := service.Descriptor(t.Context(), schedulersdk.ApplicationRef{RuntimeID: "runtime-unknown"}); err == nil {
		t.Fatal("configured Scheduler service accepted an unknown Runtime")
	}
	if _, err := service.Descriptor(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	if _, err := NewDatabaseService(DatabaseServiceOptions{
		Context: t.Context(), Database: db, Driver: "sqlite", WorkerID: "scheduler-duplicate-applications",
		Applications: []schedulersdk.ApplicationRef{ref, ref},
		Downstreams: func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error) {
			return databaseDownstream{}, nil
		},
	}); err == nil {
		t.Fatal("Scheduler service accepted a duplicate configured Runtime")
	}
}

func TestDatabaseServiceUpgradeFromV3HydratesConfiguredRuntimeWithoutRequest(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-saas-v3-upgrade-hydrate?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dialect, err := schedulerstore.Renderer("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	migrations, err := schedulerstore.SchemaMigrations("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := schedulermigration.EnsureSchema(t.Context(), db, dialect, migrations[:3]); err != nil {
		t.Fatal(err)
	}
	refA := schedulersdk.ApplicationRef{RuntimeID: "runtime-v3-a"}
	refB := schedulersdk.ApplicationRef{RuntimeID: "runtime-v3-b"}
	due := testDatabaseDefinition("legacy-due", "legacy-run-a")
	due.InitialNextRunAt = time.Now().UTC().Add(-time.Minute)
	future := testDatabaseDefinition("legacy-future", "legacy-run-b")
	future.InitialNextRunAt = time.Now().UTC().Add(time.Hour)
	for _, value := range []struct {
		ref        schedulersdk.ApplicationRef
		definition schedulersdk.Definition
		revision   int64
		id         string
	}{
		{ref: refA, definition: due, revision: 8, id: "scheduler:legacy-v3-a"},
		{ref: refB, definition: future, revision: 5, id: "scheduler:legacy-v3-b"},
	} {
		raw, marshalErr := json.Marshal(value.definition)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if _, insertErr := db.ExecContext(t.Context(), `INSERT INTO _scheduler_definitions
			(id, resource_key, object_key, name, payload_json, schema_version, schema_hash, source_kind, source_id, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			value.id, value.definition.Key, value.definition.Key, value.definition.Name, raw, fmt.Sprint(value.revision), "legacy-v3-hash", "runtime_host", value.ref.RuntimeID, "now", "now"); insertErr != nil {
			t.Fatal(insertErr)
		}
		state, stateErr := json.Marshal(value.definition)
		if stateErr != nil {
			t.Fatal(stateErr)
		}
		if _, insertErr := db.ExecContext(t.Context(), `INSERT INTO _scheduler_definition_states
			(runtime_id, definition_key, revision, enabled, definition_json, next_run_at, snapshot_revision, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, value.ref.RuntimeID, value.definition.Key, value.definition.Revision, true, state, value.definition.InitialNextRunAt.UTC().Format(time.RFC3339Nano), int64(0), time.Now().UTC().Format(time.RFC3339Nano)); insertErr != nil {
			t.Fatal(insertErr)
		}
	}
	dispatchedA := make(chan schedulersdk.Trigger, 1)
	dispatchedB := make(chan schedulersdk.Trigger, 1)
	downstreams := map[string]*recordingDatabaseDownstream{
		refA.RuntimeID: {dispatch: dispatchedA}, refB.RuntimeID: {dispatch: dispatchedB},
	}
	service, err := NewDatabaseService(DatabaseServiceOptions{
		Context: t.Context(), Database: db, Driver: "sqlite", WorkerID: "scheduler-v4-upgrade",
		Worker:       schedulersdk.WorkerConfig{Enabled: true, PollInterval: time.Millisecond, BatchSize: 10},
		Applications: []schedulersdk.ApplicationRef{refA, refB},
		Downstreams: func(_ context.Context, ref schedulersdk.ApplicationRef) (DownstreamHost, error) {
			return downstreams[ref.RuntimeID], nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Shutdown(t.Context())
	select {
	case trigger := <-dispatchedA:
		if trigger.DefinitionKey != due.Key || trigger.Target.Operation != due.Target.Operation {
			t.Fatalf("v3 runtime-a resumed wrong trigger=%+v", trigger)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("v3 active definition did not resume after v4 Scheduler-only upgrade")
	}
	select {
	case trigger := <-dispatchedB:
		t.Fatalf("v3 runtime-a restore dispatched runtime-b trigger=%+v", trigger)
	default:
	}
}

func testDatabaseDefinition(key, operation string) schedulersdk.Definition {
	return schedulersdk.Definition{
		Key: key, Name: key, Status: "enabled", Revision: "v1",
		Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60},
		Target:   schedulersdk.TargetRef{Type: "runtime_operation", Owner: "workflow", Operation: operation},
	}
}

func assertActiveDefinitions(t *testing.T, db *sql.DB, runtimeID string, want int) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _scheduler_definitions WHERE source_kind = ? AND source_id = ? AND disabled_at IS NULL`, "runtime_host", runtimeID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("runtime=%s active definitions=%d want=%d", runtimeID, count, want)
	}
}
func (databaseDownstream) ResolveHTTPConnection(context.Context, string) (modulehost.HTTPConnection, error) {
	return modulehost.HTTPConnection{}, nil
}

func TestDatabaseServicePersistsAndIsolatesSaaSApplications(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-saas?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	downstreams := map[string]*recordingDatabaseDownstream{"runtime-a": {}, "runtime-b": {}}
	service, err := NewDatabaseService(DatabaseServiceOptions{Database: db, Driver: "sqlite", WorkerID: "scheduler-saas-a", Downstreams: func(_ context.Context, ref schedulersdk.ApplicationRef) (DownstreamHost, error) {
		return downstreams[ref.RuntimeID], nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	sharedA := testDatabaseDefinition("shared", "run-a")
	sharedA.Target.Payload = json.RawMessage(`{"tenant":"a"}`)
	sharedB := testDatabaseDefinition("shared", "run-b")
	sharedB.Target.Payload = json.RawMessage(`{"tenant":"b"}`)
	onlyA := testDatabaseDefinition("a-only", "only-a")
	onlyB := testDatabaseDefinition("b-only", "only-b")
	refA := schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}
	refB := schedulersdk.ApplicationRef{RuntimeID: "runtime-b"}
	sessionA := beginDatabasePublication(t, service, refA)
	sessionB := beginDatabasePublication(t, service, refB)
	if err := service.Reconcile(t.Context(), refA, schedulersdk.DefinitionSnapshot{PublisherSession: &sessionA, Revision: 1, Definitions: []schedulersdk.Definition{sharedA, onlyA}}); err != nil {
		t.Fatal(err)
	}
	if err := service.Reconcile(t.Context(), refB, schedulersdk.DefinitionSnapshot{PublisherSession: &sessionB, Revision: 1, Definitions: []schedulersdk.Definition{sharedB, onlyB}}); err != nil {
		t.Fatal(err)
	}
	for _, input := range []struct {
		ref schedulersdk.ApplicationRef
		key string
	}{{ref: refA, key: "shared"}, {ref: refA, key: "a-only"}, {ref: refB, key: "shared"}, {ref: refB, key: "b-only"}} {
		run, triggerErr := service.TriggerNow(t.Context(), input.ref, input.key, "isolation test")
		if triggerErr != nil || run.Status != "leased" || run.DownstreamReceipt.ID != "" {
			t.Fatalf("runtime=%s key=%s run=%+v err=%v", input.ref.RuntimeID, input.key, run, triggerErr)
		}
	}
	if _, err := service.TriggerNow(t.Context(), refB, "a-only", "cross-tenant probe"); err == nil {
		t.Fatal("runtime-b executed runtime-a-only definition")
	}
	for _, check := range []struct {
		ref        schedulersdk.ApplicationRef
		operations map[string]bool
	}{{ref: refA, operations: map[string]bool{"run-a": true, "only-a": true}}, {ref: refB, operations: map[string]bool{"run-b": true, "only-b": true}}} {
		runs, listErr := service.Runs(t.Context(), check.ref, 10)
		if listErr != nil || len(runs) != 2 {
			t.Fatalf("runtime=%s runs=%+v err=%v", check.ref.RuntimeID, runs, listErr)
		}
		for _, run := range runs {
			if !check.operations[run.Trigger.Target.Operation] || run.Status != "succeeded" || run.DownstreamReceipt.ID == "" {
				t.Fatalf("runtime=%s leaked run=%+v", check.ref.RuntimeID, run)
			}
			got, getErr := service.Run(t.Context(), check.ref, run.Trigger.RunID)
			if getErr != nil || got.Trigger.Target.Operation != run.Trigger.Target.Operation {
				t.Fatalf("runtime=%s get=%+v err=%v", check.ref.RuntimeID, got, getErr)
			}
		}
	}
	for runtimeID, downstream := range downstreams {
		downstream.mu.Lock()
		if len(downstream.triggers) != 2 {
			t.Fatalf("runtime=%s downstream triggers=%+v", runtimeID, downstream.triggers)
		}
		for _, trigger := range downstream.triggers {
			if runtimeID == "runtime-a" && trigger.Target.Operation == "run-b" || runtimeID == "runtime-b" && trigger.Target.Operation == "run-a" {
				t.Fatalf("runtime=%s received cross-tenant trigger=%+v", runtimeID, trigger)
			}
		}
		downstream.mu.Unlock()
	}
	var count int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM "_scheduler_runs"`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 4 {
		t.Fatalf("scheduler SaaS run rows=%d", count)
	}
	assertActiveDefinitions(t, db, refA.RuntimeID, 2)
	assertActiveDefinitions(t, db, refB.RuntimeID, 2)
}

func beginDatabasePublication(t *testing.T, service *DatabaseService, ref schedulersdk.ApplicationRef) schedulersdk.DefinitionPublisherSession {
	t.Helper()
	session, err := service.BeginDefinitionPublisherSession(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func TestDatabaseServiceFencesPublisherTakeoverAndSurvivesRestart(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-publication-restart?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ref := schedulersdk.ApplicationRef{RuntimeID: "runtime-publication-restart"}
	newService := func(workerID string, restore bool) *DatabaseService {
		options := DatabaseServiceOptions{
			Context: t.Context(), Database: db, Driver: "sqlite", WorkerID: workerID,
			Worker: schedulersdk.WorkerConfig{Enabled: false},
			Downstreams: func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error) {
				return databaseDownstream{}, nil
			},
		}
		if restore {
			options.Applications = []schedulersdk.ApplicationRef{ref}
		}
		service, openErr := NewDatabaseService(options)
		if openErr != nil {
			t.Fatal(openErr)
		}
		return service
	}

	first := newService("scheduler-publication-before-restart", false)
	sessionA := beginDatabasePublication(t, first, ref)
	definitionA := testDatabaseDefinition("shared", "process-a")
	if err := first.Reconcile(t.Context(), ref, schedulersdk.DefinitionSnapshot{PublisherSession: &sessionA, Revision: 7, Definitions: []schedulersdk.Definition{definitionA}}); err != nil {
		t.Fatal(err)
	}
	sessionB := beginDatabasePublication(t, first, ref)
	definitionB := testDatabaseDefinition("shared", "process-b-v1")
	if err := first.Reconcile(t.Context(), ref, schedulersdk.DefinitionSnapshot{PublisherSession: &sessionB, Revision: 1, Definitions: []schedulersdk.Definition{definitionB}}); err != nil {
		t.Fatalf("new session revision 1: %v", err)
	}
	var beforeReplay string
	if err := db.QueryRowContext(t.Context(), `SELECT updated_at FROM _scheduler_definition_publications WHERE source_id = ?`, ref.RuntimeID).Scan(&beforeReplay); err != nil {
		t.Fatal(err)
	}
	if err := first.Reconcile(t.Context(), ref, schedulersdk.DefinitionSnapshot{PublisherSession: &sessionB, Revision: 1, Definitions: []schedulersdk.Definition{definitionB}}); err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	var afterReplay string
	if err := db.QueryRowContext(t.Context(), `SELECT updated_at FROM _scheduler_definition_publications WHERE source_id = ?`, ref.RuntimeID).Scan(&afterReplay); err != nil {
		t.Fatal(err)
	}
	if afterReplay != beforeReplay {
		t.Fatalf("exact replay wrote publication row: before=%s after=%s", beforeReplay, afterReplay)
	}
	conflictB := testDatabaseDefinition("shared", "process-b-conflict")
	if err := first.Reconcile(t.Context(), ref, schedulersdk.DefinitionSnapshot{PublisherSession: &sessionB, Revision: 1, Definitions: []schedulersdk.Definition{conflictB}}); !errors.Is(err, schedulersdk.ErrDefinitionSnapshotConflict) {
		t.Fatalf("same-revision conflict err=%v", err)
	}
	lateA := schedulersdk.DefinitionSnapshot{PublisherSession: &sessionA, Revision: 8, Definitions: []schedulersdk.Definition{testDatabaseDefinition("shared", "late-a")}}
	if err := first.Reconcile(t.Context(), ref, lateA); !errors.Is(err, schedulersdk.ErrDefinitionSnapshotStale) {
		t.Fatalf("late session A err=%v", err)
	}
	if err := first.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}

	second := newService("scheduler-publication-after-restart", true)
	defer second.Shutdown(t.Context())
	definitionB2 := testDatabaseDefinition("shared", "process-b-v2")
	if err := second.Reconcile(t.Context(), ref, schedulersdk.DefinitionSnapshot{PublisherSession: &sessionB, Revision: 2, Definitions: []schedulersdk.Definition{definitionB2}}); err != nil {
		t.Fatalf("active session after Scheduler restart: %v", err)
	}
	if err := second.Reconcile(t.Context(), ref, schedulersdk.DefinitionSnapshot{Revision: 3, Definitions: []schedulersdk.Definition{definitionB2}}); !errors.Is(err, schedulersdk.ErrDefinitionPublicationRequired) {
		t.Fatalf("missing session err=%v", err)
	}
	forged := sessionB
	forged.SessionNonce = "ffffffffffffffffffffffffffffffff"
	if err := second.Reconcile(t.Context(), ref, schedulersdk.DefinitionSnapshot{PublisherSession: &forged, Revision: 3, Definitions: []schedulersdk.Definition{definitionB2}}); !errors.Is(err, schedulersdk.ErrDefinitionPublicationSessionMismatch) {
		t.Fatalf("forged session err=%v", err)
	}
	var persistedHash string
	if err := db.QueryRowContext(t.Context(), `SELECT active_session_sha256 FROM _scheduler_definition_publications WHERE source_id = ?`, ref.RuntimeID).Scan(&persistedHash); err != nil {
		t.Fatal(err)
	}
	if persistedHash == sessionB.SessionNonce || len(persistedHash) != 64 {
		t.Fatalf("persisted session material=%q", persistedHash)
	}
}

func TestInvalidScheduleNeverReplacesDurableOrLiveSnapshot(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-publication-invalid-schedule?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ref := schedulersdk.ApplicationRef{RuntimeID: "runtime-invalid-schedule"}
	service, err := NewDatabaseService(DatabaseServiceOptions{
		Context: t.Context(), Database: db, Driver: "sqlite", WorkerID: "scheduler-invalid-schedule",
		Worker: schedulersdk.WorkerConfig{Enabled: false},
		Downstreams: func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error) {
			return databaseDownstream{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Shutdown(t.Context())
	session := beginDatabasePublication(t, service, ref)
	valid := testDatabaseDefinition("shared", "valid")
	if err := service.Reconcile(t.Context(), ref, schedulersdk.DefinitionSnapshot{PublisherSession: &session, Revision: 1, Definitions: []schedulersdk.Definition{valid}}); err != nil {
		t.Fatal(err)
	}
	invalid := testDatabaseDefinition("shared", "invalid")
	invalid.Schedule.IntervalSeconds = 0
	if err := service.Reconcile(t.Context(), ref, schedulersdk.DefinitionSnapshot{PublisherSession: &session, Revision: 2, Definitions: []schedulersdk.Definition{invalid}}); err == nil {
		t.Fatal("invalid schedule was accepted")
	}
	var revision int64
	var payload []byte
	if err := db.QueryRowContext(t.Context(), `SELECT revision FROM _scheduler_definition_snapshots WHERE source_id = ?`, ref.RuntimeID).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT payload_json FROM _scheduler_definitions WHERE source_id = ? AND disabled_at IS NULL`, ref.RuntimeID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if revision != 1 || !strings.Contains(string(payload), `"operation":"valid"`) {
		t.Fatalf("revision=%d payload=%s", revision, payload)
	}
	if _, err := service.TriggerNow(t.Context(), ref, "shared", "previous snapshot remains live"); err != nil {
		t.Fatalf("previous live snapshot was lost: %v", err)
	}
}

func TestConcurrentPublisherBeginLeavesOnlyHighestGenerationActive(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-publication-concurrent-begin?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	newService := func(workerID string) *DatabaseService {
		service, openErr := NewDatabaseService(DatabaseServiceOptions{
			Context: t.Context(), Database: db, Driver: "sqlite", WorkerID: workerID,
			Worker: schedulersdk.WorkerConfig{Enabled: false},
			Downstreams: func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error) {
				return databaseDownstream{}, nil
			},
		})
		if openErr != nil {
			t.Fatal(openErr)
		}
		return service
	}
	services := []*DatabaseService{newService("scheduler-publication-concurrent-a"), newService("scheduler-publication-concurrent-b")}
	defer services[0].Shutdown(t.Context())
	defer services[1].Shutdown(t.Context())
	ref := schedulersdk.ApplicationRef{RuntimeID: "runtime-concurrent-begin"}
	for _, service := range services {
		if _, err := service.Descriptor(t.Context(), ref); err != nil {
			t.Fatal(err)
		}
	}
	const count = 24
	sessions := make(chan schedulersdk.DefinitionPublisherSession, count)
	errorsByCall := make(chan error, count)
	start := make(chan struct{})
	var calls sync.WaitGroup
	for index := 0; index < count; index++ {
		service := services[index%len(services)]
		calls.Add(1)
		go func(service *DatabaseService) {
			defer calls.Done()
			<-start
			session, beginErr := service.BeginDefinitionPublisherSession(t.Context(), ref)
			if beginErr != nil {
				errorsByCall <- beginErr
				return
			}
			sessions <- session
		}(service)
	}
	close(start)
	calls.Wait()
	close(sessions)
	close(errorsByCall)
	for beginErr := range errorsByCall {
		t.Fatal(beginErr)
	}
	var highest schedulersdk.DefinitionPublisherSession
	seenGenerations := make(map[uint64]struct{}, count)
	returned := 0
	for session := range sessions {
		if err := session.Validate(); err != nil {
			t.Fatalf("invalid publisher session: %+v: %v", session, err)
		}
		if _, duplicate := seenGenerations[session.Generation]; duplicate {
			t.Fatalf("duplicate publisher generation=%d", session.Generation)
		}
		seenGenerations[session.Generation] = struct{}{}
		returned++
		if session.Generation > highest.Generation {
			highest = session
		}
	}
	if returned != count {
		t.Fatalf("returned sessions=%d want=%d", returned, count)
	}
	if highest.Generation < uint64(returned) {
		t.Fatalf("highest generation=%d is lower than successful calls=%d", highest.Generation, returned)
	}
	highestFence, err := highest.PublisherFence()
	if err != nil {
		t.Fatal(err)
	}
	var activeGeneration uint64
	var activeSessionSHA256 string
	if err := db.QueryRowContext(t.Context(), `SELECT active_generation, active_session_sha256 FROM _scheduler_definition_publications WHERE source_id = ?`, ref.RuntimeID).Scan(&activeGeneration, &activeSessionSHA256); err != nil {
		t.Fatal(err)
	}
	if activeGeneration != highestFence.Generation || activeSessionSHA256 != highestFence.SessionSHA256 {
		t.Fatalf("active fence=(%d,%s) highest returned=(%d,%s)", activeGeneration, activeSessionSHA256, highestFence.Generation, highestFence.SessionSHA256)
	}
	definition := testDatabaseDefinition("latest", "highest")
	if err := services[0].Reconcile(t.Context(), ref, schedulersdk.DefinitionSnapshot{PublisherSession: &highest, Revision: 1, Definitions: []schedulersdk.Definition{definition}}); err != nil {
		t.Fatalf("highest generation rejected: %v", err)
	}
}

func TestTwoDatabaseServicesConvergeOnHighestRevisionInOneSession(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-publication-two-services?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	newService := func(workerID string) *DatabaseService {
		service, openErr := NewDatabaseService(DatabaseServiceOptions{
			Context: t.Context(), Database: db, Driver: "sqlite", WorkerID: workerID,
			Worker: schedulersdk.WorkerConfig{Enabled: false},
			Downstreams: func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error) {
				return databaseDownstream{}, nil
			},
		})
		if openErr != nil {
			t.Fatal(openErr)
		}
		return service
	}
	first := newService("scheduler-publication-instance-a")
	defer first.Shutdown(t.Context())
	second := newService("scheduler-publication-instance-b")
	defer second.Shutdown(t.Context())
	ref := schedulersdk.ApplicationRef{RuntimeID: "runtime-two-schedulers"}
	session := beginDatabasePublication(t, first, ref)
	start := make(chan struct{})
	errorsByRevision := make(chan struct {
		revision int64
		err      error
	}, 2)
	for _, revision := range []int64{7, 6} {
		revision := revision
		service := first
		if revision == 6 {
			service = second
		}
		go func() {
			<-start
			err := service.Reconcile(t.Context(), ref, schedulersdk.DefinitionSnapshot{
				PublisherSession: &session, Revision: revision,
				Definitions: []schedulersdk.Definition{testDatabaseDefinition("shared", fmt.Sprintf("revision-%d", revision))},
			})
			errorsByRevision <- struct {
				revision int64
				err      error
			}{revision: revision, err: err}
		}()
	}
	close(start)
	for index := 0; index < 2; index++ {
		result := <-errorsByRevision
		if result.revision == 7 && result.err != nil {
			t.Fatalf("revision 7 failed: %v", result.err)
		}
		if result.revision == 6 && result.err != nil && !errors.Is(result.err, schedulersdk.ErrDefinitionSnapshotStale) {
			t.Fatalf("revision 6 err=%v", result.err)
		}
	}
	var revision int64
	var payload []byte
	if err := db.QueryRowContext(t.Context(), `SELECT revision FROM _scheduler_definition_snapshots WHERE source_id = ?`, ref.RuntimeID).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT payload_json FROM _scheduler_definitions WHERE source_id = ? AND disabled_at IS NULL`, ref.RuntimeID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if revision != 7 || !strings.Contains(string(payload), `"operation":"revision-7"`) {
		t.Fatalf("revision=%d payload=%s", revision, payload)
	}
}

func TestLegacyBindingDeleteCannotStopTakeoverWorkerOrSession(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-publication-late-close?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, err := NewDatabaseService(DatabaseServiceOptions{
		Context: t.Context(), Database: db, Driver: "sqlite", WorkerID: "scheduler-publication-close",
		Worker: schedulersdk.WorkerConfig{Enabled: true, PollInterval: time.Hour},
		Downstreams: func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error) {
			return databaseDownstream{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Shutdown(t.Context())
	ref := schedulersdk.ApplicationRef{RuntimeID: "runtime-late-close"}
	sessionA := beginDatabasePublication(t, service, ref)
	if err := service.Reconcile(t.Context(), ref, schedulersdk.DefinitionSnapshot{PublisherSession: &sessionA, Revision: 7, Definitions: []schedulersdk.Definition{testDatabaseDefinition("shared", "a")}}); err != nil {
		t.Fatal(err)
	}
	sessionB := beginDatabasePublication(t, service, ref)
	if err := service.Reconcile(t.Context(), ref, schedulersdk.DefinitionSnapshot{PublisherSession: &sessionB, Revision: 1, Definitions: []schedulersdk.Definition{testDatabaseDefinition("shared", "b-v1")}}); err != nil {
		t.Fatal(err)
	}
	handler, err := saashttp.New(saashttp.Options{ApplicationTokens: map[string]string{ref.RuntimeID: "token"}, Service: service})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodDelete, "/v1/applications/"+ref.RuntimeID+"/binding", nil)
	request.Header.Set("Authorization", "Bearer token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("late binding delete status=%d body=%s", response.Code, response.Body.String())
	}
	if err := service.Reconcile(t.Context(), ref, schedulersdk.DefinitionSnapshot{PublisherSession: &sessionB, Revision: 2, Definitions: []schedulersdk.Definition{testDatabaseDefinition("shared", "b-v2")}}); err != nil {
		t.Fatalf("session B after late close: %v", err)
	}
	service.mu.Lock()
	app := service.applications[ref.RuntimeID]
	service.mu.Unlock()
	select {
	case <-app.done:
		t.Fatal("late binding delete stopped the active worker")
	default:
	}
	if _, err := service.TriggerNow(t.Context(), ref, "shared", "still active"); err != nil {
		t.Fatalf("active takeover definition failed after late close: %v", err)
	}
}
