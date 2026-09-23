package module

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/domainry/domainry-foundation/modulehttp"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/authoring"
	"github.com/domainry/domainry-scheduler-sdk/persistence"
	"github.com/domainry/domainry-scheduler-sdk/schedule"
	schedulermodel "github.com/domainry/domainry-scheduler/internal/domain/scheduler/model"
)

const maxRequestBytes int64 = 2 << 20

type binding interface {
	DefinitionRepository() persistence.DefinitionRepository
	Preview(context.Context, schedulersdk.Schedule, time.Time, int) ([]time.Time, error)
	TriggerNow(context.Context, string, string) (schedulersdk.Run, error)
	Reschedule(context.Context, string, time.Time, string) error
	Runs(context.Context, int) ([]schedulersdk.Run, error)
	Run(context.Context, string) (schedulersdk.Run, error)
	RetryRun(context.Context, string, string) (schedulersdk.Run, error)
	CancelRun(context.Context, string, string) (schedulersdk.Run, error)
	DeadLetters(context.Context, int) ([]schedulersdk.DeadLetter, error)
	ResolveDeadLetter(context.Context, string, string) (schedulersdk.DeadLetter, error)
	RequeueDeadLetter(context.Context, string, string) (schedulersdk.Run, error)
}

type adapter struct {
	binding  binding
	receipts schedulermodel.CommandReceiptStore
	handler  http.Handler
	routes   []modulehttp.Route
}

func (*adapter) ContractVersion() string { return modulehttp.ContractVersion }
func (*adapter) Owner() string           { return "scheduler" }
func (*adapter) Name() string            { return "scheduler_external" }
func (a *adapter) Handler() http.Handler { return a.handler }
func (a *adapter) Routes() []modulehttp.Route {
	return append([]modulehttp.Route(nil), a.routes...)
}
func NewAdapter(owner binding, receipts schedulermodel.CommandReceiptStore) (modulehttp.Adapter, error) {
	if owner == nil || owner.DefinitionRepository() == nil {
		return nil, errors.New("Scheduler HTTP binding is unavailable")
	}
	if receipts == nil {
		return nil, errors.New("Scheduler command receipt store is unavailable")
	}
	contract, err := schedulersdk.SchedulerHTTPAdapterContractForTrustedHost(schedulersdk.SchedulerHTTPAuthorizationBoundary{
		TrustedHumanPrincipal: true, ActionPermissionGuard: true, OperationEvidenceGuard: true,
	})
	if err != nil {
		return nil, err
	}
	a := &adapter{binding: owner, receipts: receipts, routes: make([]modulehttp.Route, 0, len(contract.Routes))}
	handlers := a.handlers()
	mux := http.NewServeMux()
	for _, declared := range contract.Routes {
		route, projectErr := modulehttp.RouteFromAction(declared.Action)
		if projectErr != nil {
			return nil, fmt.Errorf("project Scheduler Action %q: %w", declared.Action.Key, projectErr)
		}
		handler, found := handlers[route.Action.Key]
		if !found {
			return nil, fmt.Errorf("Scheduler Action %q has no HTTP handler", route.Action.Key)
		}
		a.routes = append(a.routes, route)
		if schedulerCommandActions[route.Action.Key] {
			handler = a.withCommandReceipt(route.Action.Key, handler)
		}
		mux.HandleFunc(route.Pattern(), handler)
		delete(handlers, route.Action.Key)
	}
	if len(handlers) != 0 {
		keys := make([]string, 0, len(handlers))
		for key := range handlers {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		return nil, fmt.Errorf("Scheduler HTTP implementations differ from the Action contract: %v", keys)
	}
	a.handler = mux
	if err := modulehttp.ValidateAdapter(a); err != nil {
		return nil, err
	}
	return a, nil
}

var schedulerCommandActions = map[string]bool{
	schedulersdk.ActionSchedulerDefinitionsRun:        true,
	schedulersdk.ActionSchedulerDefinitionsReschedule: true,
	schedulersdk.ActionSchedulerRunsRetry:             true,
	schedulersdk.ActionSchedulerRunsCancel:            true,
	schedulersdk.ActionSchedulerDeadLettersResolve:    true,
	schedulersdk.ActionSchedulerDeadLettersRequeue:    true,
}

func (a *adapter) withCommandReceipt(actionKey string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		if idempotencyKey == "" {
			writeError(w, http.StatusBadRequest, "backend.scheduler.idempotency_key_required")
			return
		}
		if len(idempotencyKey) > 191 {
			writeError(w, http.StatusBadRequest, "backend.scheduler.idempotency_key_invalid")
			return
		}
		requestHash, err := schedulerCommandRequestHash(r, actionKey)
		if err != nil {
			if errors.Is(err, errRequestTooLarge) {
				writeError(w, http.StatusRequestEntityTooLarge, "backend.request_body_too_large")
				return
			}
			writeError(w, http.StatusBadRequest, "backend.invalid_json")
			return
		}
		resourceKey := strings.TrimSpace(r.URL.Path)
		claim := schedulermodel.CommandReceipt{IdempotencyKey: idempotencyKey, ActionKey: actionKey, ResourceKey: resourceKey, RequestHash: requestHash}
		receipt, claimed, err := a.receipts.ClaimCommand(r.Context(), claim)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "backend.scheduler.idempotency_unavailable")
			return
		}
		if !claimed {
			if receipt.ActionKey != actionKey || receipt.ResourceKey != resourceKey || receipt.RequestHash != requestHash {
				writeError(w, http.StatusConflict, "backend.scheduler.idempotency_conflict")
				return
			}
			if receipt.Status != schedulermodel.CommandReceiptCompleted {
				writeError(w, http.StatusConflict, "backend.scheduler.operation_in_progress")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Idempotency-Replayed", "true")
			w.WriteHeader(receipt.HTTPStatus)
			_, _ = w.Write(receipt.ResponseJSON)
			return
		}

		buffer := newBufferedResponse()
		next(buffer, r)
		completeCtx, cancelComplete := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
		defer cancelComplete()
		if err := a.receipts.CompleteCommand(completeCtx, idempotencyKey, requestHash, buffer.status, buffer.body.Bytes()); err != nil {
			writeError(w, http.StatusServiceUnavailable, "backend.scheduler.idempotency_unavailable")
			return
		}
		copyResponse(w, buffer)
	}
}

var errRequestTooLarge = errors.New("Scheduler request body is too large")

func schedulerCommandRequestHash(r *http.Request, actionKey string) (string, error) {
	body := []byte{}
	if r.Body != nil {
		value, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBytes+1))
		if err != nil {
			return "", err
		}
		if int64(len(value)) > maxRequestBytes {
			return "", errRequestTooLarge
		}
		body = bytes.TrimSpace(value)
		r.Body = io.NopCloser(bytes.NewReader(value))
	}
	canonicalBody := body
	if len(body) != 0 {
		var value any
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return "", err
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			return "", errors.New("request body has trailing JSON")
		}
		canonicalBody, _ = json.Marshal(value)
	}
	digest := sha256.Sum256([]byte(strings.Join([]string{
		strings.TrimSpace(actionKey),
		r.Method,
		strings.TrimSpace(r.URL.Path),
		strings.TrimSpace(r.Header.Get("X-Operation-Reason")),
		strings.TrimSpace(r.Header.Get("X-Operation-Confirmation")),
		string(canonicalBody),
	}, "\n")))
	return fmt.Sprintf("%x", digest[:]), nil
}

type bufferedResponseWriter struct {
	header      http.Header
	status      int
	wroteHeader bool
	body        bytes.Buffer
}

func newBufferedResponse() *bufferedResponseWriter {
	return &bufferedResponseWriter{header: make(http.Header), status: http.StatusOK}
}

func (w *bufferedResponseWriter) Header() http.Header { return w.header }
func (w *bufferedResponseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.status, w.wroteHeader = status, true
}
func (w *bufferedResponseWriter) Write(value []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.body.Write(value)
}

func copyResponse(target http.ResponseWriter, source *bufferedResponseWriter) {
	for key, values := range source.header {
		for _, value := range values {
			target.Header().Add(key, value)
		}
	}
	target.WriteHeader(source.status)
	_, _ = target.Write(source.body.Bytes())
}

func (a *adapter) handlers() map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		schedulersdk.ActionSchedulerDefinitionsList:       a.listDefinitions,
		schedulersdk.ActionSchedulerDefinitionsGet:        a.getDefinition,
		schedulersdk.ActionSchedulerAuthoringContractGet:  a.getAuthoringContract,
		schedulersdk.ActionSchedulerDefinitionsValidate:   a.previewDefinition,
		schedulersdk.ActionSchedulerSchedulesPreview:      a.previewSchedule,
		schedulersdk.ActionSchedulerDefinitionsSimulate:   a.simulateDefinition,
		schedulersdk.ActionSchedulerStateGet:              a.getState,
		schedulersdk.ActionSchedulerDefinitionsRun:        a.runDefinition,
		schedulersdk.ActionSchedulerDefinitionsReschedule: a.rescheduleDefinition,
		schedulersdk.ActionSchedulerRunsRetry:             a.retryRun,
		schedulersdk.ActionSchedulerRunsCancel:            a.cancelRun,
		schedulersdk.ActionSchedulerDeadLettersResolve:    a.resolveDeadLetter,
		schedulersdk.ActionSchedulerDeadLettersRequeue:    a.requeueDeadLetter,
	}
}

func (a *adapter) listDefinitions(w http.ResponseWriter, r *http.Request) {
	snapshot, err := a.binding.DefinitionRepository().DefinitionSnapshot(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	items := make([]authoring.ManagementDefinition, 0, len(snapshot.Definitions))
	for _, definition := range snapshot.Definitions {
		items = append(items, projectDefinition(definition))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}

func (a *adapter) getDefinition(w http.ResponseWriter, r *http.Request) {
	definition, found, err := a.definition(r.Context(), r.PathValue("definitionID"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "backend.scheduler.definition_not_found")
		return
	}
	writeJSON(w, http.StatusOK, projectDefinition(definition))
}

func (*adapter) getAuthoringContract(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, authoring.ManagementContract())
}

func (a *adapter) previewDefinition(w http.ResponseWriter, r *http.Request) {
	request := struct {
		Data map[string]any `json:"data"`
	}{}
	if !decodeJSON(w, r, &request, true) {
		return
	}
	next, err := schedule.PreviewDefinitionData(r.Context(), request.Data, time.Now().UTC(), 3)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"next_runs": formatTimes(next)})
}

func (a *adapter) previewSchedule(w http.ResponseWriter, r *http.Request) {
	request := map[string]any{}
	if !decodeJSON(w, r, &request, true) {
		return
	}
	next, err := schedule.PreviewData(r.Context(), request, time.Now().UTC(), 3)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"next_runs": formatTimes(next)})
}

func (a *adapter) simulateDefinition(w http.ResponseWriter, r *http.Request) {
	definition, found, err := a.definition(r.Context(), r.PathValue("definitionID"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "backend.scheduler.definition_not_found")
		return
	}
	next, err := a.binding.Preview(r.Context(), definition.Schedule, time.Now().UTC(), 1)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	nextRun := ""
	if len(next) != 0 {
		nextRun = next[0].UTC().Format(time.RFC3339)
	}
	targetType, targetKey := projectTarget(definition.Target)
	writeJSON(w, http.StatusOK, map[string]any{"status": "valid", "message": "Scheduler definition is valid", "next_run_at": nextRun, "target_type": targetType, "target_key": targetKey})
}

func (a *adapter) getState(w http.ResponseWriter, r *http.Request) {
	runs, err := a.binding.Runs(r.Context(), 500)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	deadLetters, err := a.binding.DeadLetters(r.Context(), 500)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	projectedRuns := make([]runResponse, 0, len(runs))
	for _, run := range runs {
		projectedRuns = append(projectedRuns, projectRun(run))
	}
	projectedDeadLetters := make([]deadLetterResponse, 0, len(deadLetters))
	for _, deadLetter := range deadLetters {
		projectedDeadLetters = append(projectedDeadLetters, projectDeadLetter(deadLetter))
	}
	writeJSON(w, http.StatusOK, map[string]any{"provisioned": true, "runs": projectedRuns, "dead_letters": projectedDeadLetters})
}

func (a *adapter) runDefinition(w http.ResponseWriter, r *http.Request) {
	run, err := a.binding.TriggerNow(r.Context(), strings.TrimSpace(r.PathValue("definitionID")), operationReason(r))
	if err == nil && strings.TrimSpace(run.Trigger.RunID) != "" {
		if current, getErr := a.binding.Run(r.Context(), run.Trigger.RunID); getErr == nil {
			run = current
		}
	}
	writeCommandResult(w, projectRun(run), err)
}

func (a *adapter) rescheduleDefinition(w http.ResponseWriter, r *http.Request) {
	request := struct {
		NextRunAt string `json:"next_run_at"`
	}{}
	if !decodeJSON(w, r, &request, true) {
		return
	}
	nextRunAt, err := time.Parse(time.RFC3339, strings.TrimSpace(request.NextRunAt))
	if err != nil {
		writeError(w, http.StatusBadRequest, "backend.scheduler.next_run_at_invalid")
		return
	}
	key := strings.TrimSpace(r.PathValue("definitionID"))
	err = a.binding.Reschedule(r.Context(), key, nextRunAt, operationReason(r))
	writeCommandResult(w, map[string]any{"status": "rescheduled", "definition_key": key, "next_run_at": nextRunAt.UTC().Format(time.RFC3339)}, err)
}

func (a *adapter) retryRun(w http.ResponseWriter, r *http.Request) {
	value, err := a.binding.RetryRun(r.Context(), strings.TrimSpace(r.PathValue("runID")), operationReason(r))
	writeCommandResult(w, projectRun(value), err)
}

func (a *adapter) cancelRun(w http.ResponseWriter, r *http.Request) {
	value, err := a.binding.CancelRun(r.Context(), strings.TrimSpace(r.PathValue("runID")), operationReason(r))
	writeCommandResult(w, projectRun(value), err)
}

func (a *adapter) resolveDeadLetter(w http.ResponseWriter, r *http.Request) {
	note, ok := optionalNote(w, r)
	if !ok {
		return
	}
	value, err := a.binding.ResolveDeadLetter(r.Context(), strings.TrimSpace(r.PathValue("deadLetterID")), firstNonEmpty(note, operationReason(r)))
	writeCommandResult(w, projectDeadLetter(value), err)
}

func (a *adapter) requeueDeadLetter(w http.ResponseWriter, r *http.Request) {
	note, ok := optionalNote(w, r)
	if !ok {
		return
	}
	value, err := a.binding.RequeueDeadLetter(r.Context(), strings.TrimSpace(r.PathValue("deadLetterID")), firstNonEmpty(note, operationReason(r)))
	writeCommandResult(w, projectRun(value), err)
}

func (a *adapter) definition(ctx context.Context, key string) (schedulersdk.Definition, bool, error) {
	snapshot, err := a.binding.DefinitionRepository().DefinitionSnapshot(ctx)
	if err != nil {
		return schedulersdk.Definition{}, false, err
	}
	key = strings.TrimSpace(key)
	for _, definition := range snapshot.Definitions {
		if strings.TrimSpace(definition.Key) == key {
			return definition, true, nil
		}
	}
	return schedulersdk.Definition{}, false, nil
}

func projectDefinition(value schedulersdk.Definition) authoring.ManagementDefinition {
	targetType, targetKey := projectTarget(value.Target)
	result := authoring.ManagementDefinition{
		Key: value.Key, Name: value.Name, Description: value.Description, I18n: cloneRawMessages(value.I18n), Status: value.Status,
		ScheduleType: value.Schedule.Type, ScheduleExpression: value.Schedule.Expression,
		IntervalSeconds: value.Schedule.IntervalSeconds, TimeOfDay: value.Schedule.TimeOfDay,
		DayOfWeek: value.Schedule.DayOfWeek, DayOfMonth: value.Schedule.DayOfMonth, Timezone: value.Schedule.Timezone,
		TargetType: targetType, TargetKey: targetKey, TargetObject: value.Target.ObjectKey, RunAsRole: value.Target.RunAsRole, ConnectionKey: value.Target.ConnectionKey, MaxAttempts: value.Policy.MaxAttempts,
		TimeoutSeconds: int(value.Policy.Timeout / time.Second), MissedWindowPolicy: value.Policy.Misfire,
		MaxCatchupWindows: value.Policy.MaxCatchupWindows, RetryDelaySeconds: int(value.Policy.RetryInitial / time.Second),
		RetryMaxDelay: int(value.Policy.RetryMax / time.Second), PayloadJSON: string(value.Target.Payload),
	}
	if !value.InitialNextRunAt.IsZero() {
		result.NextRunAt = value.InitialNextRunAt.UTC().Format(time.RFC3339)
	}
	return result
}

func cloneRawMessages(values map[string]json.RawMessage) map[string]json.RawMessage {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]json.RawMessage, len(values))
	for key, value := range values {
		result[key] = append(json.RawMessage(nil), value...)
	}
	return result
}

func projectTarget(value schedulersdk.TargetRef) (string, string) {
	if strings.TrimSpace(value.Type) == "runtime_operation" {
		return strings.TrimSpace(value.Owner), strings.TrimSpace(value.Operation)
	}
	return strings.TrimSpace(value.Type), strings.TrimSpace(value.Operation)
}

type runResponse struct {
	ID                string                     `json:"id"`
	DefinitionKey     string                     `json:"definition_key,omitempty"`
	Status            string                     `json:"status"`
	Attempt           int                        `json:"attempt,omitempty"`
	ScheduledFor      string                     `json:"scheduled_for,omitempty"`
	WindowKey         string                     `json:"window_key,omitempty"`
	ErrorMessage      string                     `json:"error_message,omitempty"`
	LeaseOwner        string                     `json:"lease_owner,omitempty"`
	LeaseExpiresAt    string                     `json:"lease_expires_at,omitempty"`
	FencingToken      int64                      `json:"fencing_token,omitempty"`
	CorrelationID     string                     `json:"correlation_id,omitempty"`
	DownstreamReceipt *downstreamReceiptResponse `json:"downstream_receipt,omitempty"`
	CreatedAt         string                     `json:"created_at,omitempty"`
	UpdatedAt         string                     `json:"updated_at,omitempty"`
}

type downstreamReceiptResponse struct {
	ID     string `json:"id"`
	Owner  string `json:"owner,omitempty"`
	Status string `json:"status"`
	Replay bool   `json:"replay"`
}

func projectRun(value schedulersdk.Run) runResponse {
	return runResponse{ID: value.Trigger.RunID, DefinitionKey: value.Trigger.DefinitionKey, Status: value.Status,
		Attempt: value.Trigger.Attempt, ScheduledFor: formatTime(value.Trigger.ScheduledFor), WindowKey: value.Trigger.WindowKey, ErrorMessage: value.LastError,
		LeaseOwner: value.Lease.Owner, LeaseExpiresAt: formatTime(value.Lease.ExpiresAt), FencingToken: value.Lease.Token,
		CorrelationID: value.DownstreamReceipt.ID, DownstreamReceipt: projectDownstreamReceipt(value.DownstreamReceipt), CreatedAt: formatTime(value.CreatedAt), UpdatedAt: formatTime(value.UpdatedAt)}
}

func projectDownstreamReceipt(value schedulersdk.DownstreamReceipt) *downstreamReceiptResponse {
	if strings.TrimSpace(value.ID) == "" {
		return nil
	}
	return &downstreamReceiptResponse{ID: value.ID, Owner: value.Owner, Status: value.Status, Replay: value.Replay}
}

type deadLetterResponse struct {
	ID            string `json:"id"`
	RunID         string `json:"run_id"`
	DefinitionKey string `json:"definition_key,omitempty"`
	Status        string `json:"status"`
	Reason        string `json:"reason,omitempty"`
	FailedAt      string `json:"failed_at,omitempty"`
	ResolvedAt    string `json:"resolved_at,omitempty"`
}

func projectDeadLetter(value schedulersdk.DeadLetter) deadLetterResponse {
	return deadLetterResponse{ID: value.RunID, RunID: value.RunID, DefinitionKey: value.DefinitionKey, Status: value.Status,
		Reason: value.Reason, FailedAt: formatTime(value.FailedAt), ResolvedAt: formatTime(value.ResolvedAt)}
}

func optionalNote(w http.ResponseWriter, r *http.Request) (string, bool) {
	if r.Body == nil || r.ContentLength == 0 {
		return "", true
	}
	request := struct {
		Note string `json:"note"`
	}{}
	if !decodeJSON(w, r, &request, false) {
		return "", false
	}
	return strings.TrimSpace(request.Note), true
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any, required bool) bool {
	if r.Body == nil {
		if required {
			writeError(w, http.StatusBadRequest, "backend.request_body_required")
			return false
		}
		return true
	}
	reader := io.LimitReader(r.Body, maxRequestBytes+1)
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		if !required && errors.Is(err, io.EOF) {
			return true
		}
		writeError(w, http.StatusBadRequest, "backend.invalid_json")
		return false
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "backend.invalid_json")
		return false
	}
	return true
}

func writeCommandResult(w http.ResponseWriter, value any, err error) {
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func writeServiceError(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	if code := schedule.ValidationCode(err); code != "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"code": code, "params": schedule.ValidationParams(err)})
		return
	}
	if errors.Is(err, schedulersdk.ErrTriggerBacklogFull) {
		writeError(w, http.StatusTooManyRequests, "backend.scheduler.trigger_backlog_full")
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		writeError(w, http.StatusServiceUnavailable, "backend.scheduler.request_cancelled")
		return
	}
	writeJSON(w, http.StatusInternalServerError, map[string]any{"code": "backend.scheduler.request_failed", "message": err.Error()})
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]any{"code": code})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func operationReason(r *http.Request) string {
	return strings.TrimSpace(r.Header.Get("X-Operation-Reason"))
}

func formatTimes(values []time.Time) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, value.UTC().Format(time.RFC3339))
	}
	return result
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if result := strings.TrimSpace(value); result != "" {
			return result
		}
	}
	return ""
}

var _ modulehttp.Adapter = (*adapter)(nil)
