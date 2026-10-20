package saas

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/domainry/domainry-foundation/httpresponse"
	"github.com/domainry/domainry-foundation/telemetry"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/saashost"
)

type Service interface {
	Descriptor(context.Context, schedulersdk.ApplicationRef) (schedulersdk.Descriptor, error)
	BeginDefinitionPublisherSession(context.Context, schedulersdk.ApplicationRef) (schedulersdk.DefinitionPublisherSession, error)
	Reconcile(context.Context, schedulersdk.ApplicationRef, schedulersdk.DefinitionSnapshot) error
	Preview(context.Context, schedulersdk.ApplicationRef, schedulersdk.Schedule, time.Time, int) ([]time.Time, error)
	Tick(context.Context, schedulersdk.ApplicationRef, time.Time, int) (int, error)
	TriggerNow(context.Context, schedulersdk.ApplicationRef, string, string) (schedulersdk.Run, error)
	Reschedule(context.Context, schedulersdk.ApplicationRef, string, time.Time, string) error
	Runs(context.Context, schedulersdk.ApplicationRef, int) ([]schedulersdk.Run, error)
	Run(context.Context, schedulersdk.ApplicationRef, string) (schedulersdk.Run, error)
	RetryRun(context.Context, schedulersdk.ApplicationRef, string, string) (schedulersdk.Run, error)
	CancelRun(context.Context, schedulersdk.ApplicationRef, string, string) (schedulersdk.Run, error)
	DeadLetters(context.Context, schedulersdk.ApplicationRef, int) ([]schedulersdk.DeadLetter, error)
	DeadLetter(context.Context, schedulersdk.ApplicationRef, string) (schedulersdk.DeadLetter, error)
	ResolveDeadLetter(context.Context, schedulersdk.ApplicationRef, string, string) (schedulersdk.DeadLetter, error)
	RequeueDeadLetter(context.Context, schedulersdk.ApplicationRef, string, string) (schedulersdk.Run, error)
	Close(context.Context, schedulersdk.ApplicationRef) error
}

type scheduledPlanService interface {
	CreateScheduledPlan(context.Context, schedulersdk.ApplicationRef, schedulersdk.ScheduledPlanCreate) (schedulersdk.ScheduledPlanReceipt, error)
	GetScheduledPlan(context.Context, schedulersdk.ApplicationRef, schedulersdk.ScheduledPlanLookup) (schedulersdk.ScheduledPlan, error)
	ListScheduledPlans(context.Context, schedulersdk.ApplicationRef, schedulersdk.ScheduledPlanList) (schedulersdk.ScheduledPlanPage, error)
	UpdateScheduledPlan(context.Context, schedulersdk.ApplicationRef, schedulersdk.ScheduledPlanUpdate) (schedulersdk.ScheduledPlanReceipt, error)
	PauseScheduledPlan(context.Context, schedulersdk.ApplicationRef, schedulersdk.ScheduledPlanStatusChange) (schedulersdk.ScheduledPlanReceipt, error)
	ResumeScheduledPlan(context.Context, schedulersdk.ApplicationRef, schedulersdk.ScheduledPlanStatusChange) (schedulersdk.ScheduledPlanReceipt, error)
	DeleteScheduledPlan(context.Context, schedulersdk.ApplicationRef, schedulersdk.ScheduledPlanStatusChange) (schedulersdk.ScheduledPlanDeleteReceipt, error)
}

type Options struct {
	// ApplicationTokens binds each private Runtime credential to exactly one
	// Scheduler application. Request headers and URL parameters never select an
	// application independently of this authenticated identity.
	ApplicationTokens map[string]string
	Service           Service
}

type Server struct {
	credentials []applicationCredential
	service     Service
}

type applicationCredential struct {
	application schedulersdk.ApplicationRef
	tokenDigest [sha256.Size]byte
}

func New(options Options) (*Server, error) {
	if options.Service == nil {
		return nil, fmt.Errorf("Scheduler service is required")
	}
	credentials := make([]applicationCredential, 0, len(options.ApplicationTokens))
	owners := make(map[[sha256.Size]byte]string, len(options.ApplicationTokens))
	for runtimeID, rawToken := range options.ApplicationTokens {
		application := schedulersdk.ApplicationRef{RuntimeID: strings.TrimSpace(runtimeID)}
		if err := application.Validate(); err != nil {
			return nil, fmt.Errorf("Scheduler SaaS application credential has invalid Runtime identity: %w", err)
		}
		token := strings.TrimSpace(rawToken)
		if token == "" {
			return nil, fmt.Errorf("Scheduler SaaS application credential for %q is empty", application.RuntimeID)
		}
		digest := sha256.Sum256([]byte(token))
		if owner := owners[digest]; owner != "" && owner != application.RuntimeID {
			return nil, fmt.Errorf("Scheduler SaaS bearer credential cannot be shared by Runtime %q and %q", owner, application.RuntimeID)
		}
		owners[digest] = application.RuntimeID
		credentials = append(credentials, applicationCredential{application: application, tokenDigest: digest})
	}
	if len(credentials) == 0 {
		return nil, fmt.Errorf("Scheduler SaaS application credentials are required")
	}
	return &Server{credentials: credentials, service: options.Service}, nil
}
func (s *Server) Routes() http.Handler { return s }

func (s *Server) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if route := schedulerRoutePattern(request.Method, request.URL.Path); route != "" {
		telemetry.RecordHTTPRoute(request, route)
	}
	// The source-owned Action adapter is safe in Module mode because Runtime's
	// listener applies the Action permission and operation guards. Standalone
	// SaaS has no equivalent trusted human principal contract, so it must not
	// mount the /scheduler surface behind a machine credential.
	if request.URL.Path == "/scheduler" || strings.HasPrefix(request.URL.Path, "/scheduler/") {
		httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusNotFound, Code: "scheduler.route_not_found"})
		return
	}
	authenticated, ok := s.authenticate(request)
	if !ok {
		httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusUnauthorized, Code: "scheduler.service_credential_invalid"})
		return
	}
	parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
	if len(parts) < 4 || len(parts) > 6 || parts[0] != "v1" || parts[1] != "applications" {
		httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusNotFound, Code: "scheduler.route_not_found"})
		return
	}
	requested := schedulersdk.ApplicationRef{RuntimeID: strings.TrimSpace(parts[2])}
	if requested.Validate() != nil {
		httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusBadRequest, Code: "scheduler.application_invalid"})
		return
	}
	if requested.RuntimeID != authenticated.RuntimeID {
		httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusForbidden, Code: "scheduler.application_forbidden"})
		return
	}
	// Pass only the identity resolved from the credential. The path and legacy
	// X-Domainry-Runtime-ID header are never application-selection authority.
	application := authenticated
	resource := parts[3]
	var value any
	var err error
	switch resource {
	case "descriptor":
		if request.Method != http.MethodGet {
			break
		}
		value, err = s.service.Descriptor(request.Context(), application)
	case "definitions":
		if len(parts) == 4 && request.Method == http.MethodPut {
			var snapshot schedulersdk.DefinitionSnapshot
			if !decode(response, request, &snapshot) {
				return
			}
			err = s.service.Reconcile(request.Context(), application, snapshot)
			if err == nil {
				response.WriteHeader(http.StatusNoContent)
				return
			}
			break
		}
		if len(parts) == 6 && parts[5] == "reschedule" && request.Method == http.MethodPost {
			var input struct {
				NextRunAt time.Time `json:"next_run_at"`
				Reason    string    `json:"reason"`
			}
			if !decode(response, request, &input) {
				return
			}
			err = s.service.Reschedule(request.Context(), application, parts[4], input.NextRunAt, input.Reason)
			if err == nil {
				response.WriteHeader(http.StatusNoContent)
				return
			}
		}
	case "plans":
		plans, ok := s.service.(scheduledPlanService)
		if !ok {
			httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusServiceUnavailable, Code: "scheduler.plan_records_unavailable"})
			return
		}
		if len(parts) == 4 && request.Method == http.MethodPost {
			var input schedulersdk.ScheduledPlanCreate
			if !decode(response, request, &input) {
				return
			}
			value, err = plans.CreateScheduledPlan(request.Context(), application, input)
			break
		}
		if len(parts) == 4 && request.Method == http.MethodGet {
			limit, limitErr := strconv.Atoi(request.URL.Query().Get("limit"))
			if request.URL.Query().Get("limit") != "" && limitErr != nil {
				err = schedulersdk.ErrScheduledPlanInvalid
				break
			}
			value, err = plans.ListScheduledPlans(request.Context(), application, schedulersdk.ScheduledPlanList{
				Owner: schedulersdk.ScheduledPlanOwner{
					WorkspaceID: request.URL.Query().Get("workspace_id"), UserID: request.URL.Query().Get("user_id"), ProductKey: request.URL.Query().Get("product_key"),
				},
				Status: request.URL.Query().Get("status"), Cursor: request.URL.Query().Get("cursor"), Limit: limit,
			})
			break
		}
		if len(parts) == 6 && parts[5] == "deletion-receipt" && request.Method == http.MethodGet {
			reader, ok := s.service.(saashost.ScheduledPlanDeletionTransport)
			if !ok {
				err = schedulersdk.ErrScheduledPlanDeletionReadUnsupported
				break
			}
			value, err = reader.ReadScheduledPlanDeletion(request.Context(), application, schedulersdk.ScheduledPlanLookup{
				PlanID: parts[4], Owner: schedulersdk.ScheduledPlanOwner{
					WorkspaceID: request.URL.Query().Get("workspace_id"), UserID: request.URL.Query().Get("user_id"), ProductKey: request.URL.Query().Get("product_key"),
				},
			})
			break
		}
		if len(parts) == 5 && request.Method == http.MethodGet {
			lookup := schedulersdk.ScheduledPlanLookup{
				PlanID: parts[4],
				Owner: schedulersdk.ScheduledPlanOwner{
					WorkspaceID: request.URL.Query().Get("workspace_id"),
					UserID:      request.URL.Query().Get("user_id"),
					ProductKey:  request.URL.Query().Get("product_key"),
				},
			}
			value, err = plans.GetScheduledPlan(request.Context(), application, lookup)
			break
		}
		if len(parts) == 5 && request.Method == http.MethodPut {
			var input schedulersdk.ScheduledPlanUpdate
			if !decode(response, request, &input) {
				return
			}
			if input.PlanID != parts[4] {
				err = schedulersdk.ErrScheduledPlanInvalid
				break
			}
			value, err = plans.UpdateScheduledPlan(request.Context(), application, input)
			break
		}
		if len(parts) == 5 && request.Method == http.MethodDelete {
			var input schedulersdk.ScheduledPlanStatusChange
			if !decode(response, request, &input) {
				return
			}
			if input.PlanID != parts[4] {
				err = schedulersdk.ErrScheduledPlanInvalid
				break
			}
			value, err = plans.DeleteScheduledPlan(request.Context(), application, input)
			break
		}
		if len(parts) == 6 && request.Method == http.MethodPost {
			var input schedulersdk.ScheduledPlanStatusChange
			if !decode(response, request, &input) {
				return
			}
			if input.PlanID != parts[4] {
				err = schedulersdk.ErrScheduledPlanInvalid
				break
			}
			switch parts[5] {
			case "pause":
				value, err = plans.PauseScheduledPlan(request.Context(), application, input)
			case "resume":
				value, err = plans.ResumeScheduledPlan(request.Context(), application, input)
			}
		}
	case "definition-publication-sessions":
		if len(parts) != 4 || request.Method != http.MethodPost {
			break
		}
		value, err = s.service.BeginDefinitionPublisherSession(request.Context(), application)
	case "preview":
		if request.Method != http.MethodPost {
			break
		}
		var input struct {
			Schedule schedulersdk.Schedule `json:"schedule"`
			After    time.Time             `json:"after"`
			Count    int                   `json:"count"`
		}
		if !decode(response, request, &input) {
			return
		}
		var next []time.Time
		next, err = s.service.Preview(request.Context(), application, input.Schedule, input.After, input.Count)
		value = map[string]any{"next": next}
	case "ticks":
		if request.Method != http.MethodPost {
			break
		}
		var input struct {
			Now   time.Time `json:"now"`
			Limit int       `json:"limit"`
		}
		if !decode(response, request, &input) {
			return
		}
		var processed int
		processed, err = s.service.Tick(request.Context(), application, input.Now, input.Limit)
		value = map[string]int{"processed": processed}
	case "triggers":
		if request.Method != http.MethodPost {
			break
		}
		var input struct {
			DefinitionKey string `json:"definition_key"`
			Reason        string `json:"reason"`
		}
		if !decode(response, request, &input) {
			return
		}
		value, err = s.service.TriggerNow(request.Context(), application, input.DefinitionKey, input.Reason)
	case "runs":
		if len(parts) == 4 && request.Method == http.MethodGet {
			limit, _ := strconv.Atoi(request.URL.Query().Get("limit"))
			var items []schedulersdk.Run
			items, err = s.service.Runs(request.Context(), application, limit)
			value = map[string]any{"items": items}
			break
		}
		if len(parts) < 5 {
			break
		}
		id := parts[4]
		if len(parts) == 5 && request.Method == http.MethodGet {
			value, err = s.service.Run(request.Context(), application, id)
			break
		}
		if len(parts) == 6 && request.Method == http.MethodPost {
			var input struct {
				Reason string `json:"reason"`
			}
			if !decode(response, request, &input) {
				return
			}
			switch parts[5] {
			case "retry":
				value, err = s.service.RetryRun(request.Context(), application, id, input.Reason)
			case "cancel":
				value, err = s.service.CancelRun(request.Context(), application, id, input.Reason)
			}
		}
	case "dead-letters":
		if len(parts) == 4 && request.Method == http.MethodGet {
			limit, _ := strconv.Atoi(request.URL.Query().Get("limit"))
			var items []schedulersdk.DeadLetter
			items, err = s.service.DeadLetters(request.Context(), application, limit)
			value = map[string]any{"items": items}
			break
		}
		if len(parts) < 5 {
			break
		}
		id := parts[4]
		if len(parts) == 5 && request.Method == http.MethodGet {
			value, err = s.service.DeadLetter(request.Context(), application, id)
			break
		}
		if len(parts) == 6 && request.Method == http.MethodPost {
			var input struct {
				Reason string `json:"reason"`
			}
			if !decode(response, request, &input) {
				return
			}
			switch parts[5] {
			case "resolve":
				value, err = s.service.ResolveDeadLetter(request.Context(), application, id, input.Reason)
			case "requeue":
				value, err = s.service.RequeueDeadLetter(request.Context(), application, id, input.Reason)
			}
		}
	case "binding":
		// Retained as a no-op only for rolling clients. Publisher sessions fence
		// configuration; a stale Runtime must not own Scheduler worker lifetime.
		if request.Method == http.MethodDelete {
			response.WriteHeader(http.StatusNoContent)
			return
		}
	default:
		httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusNotFound, Code: "scheduler.route_not_found"})
		return
	}
	if err != nil {
		if errors.Is(err, schedulersdk.ErrScheduledPlanDeletionReadUnsupported) {
			httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusNotImplemented, Code: "scheduler.plan_deletion_read_unsupported", Cause: err})
			return
		}
		if errors.Is(err, schedulersdk.ErrTriggerBacklogFull) {
			httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusTooManyRequests, Code: "backend.scheduler.trigger_backlog_full", Cause: err})
			return
		}
		if errors.Is(err, schedulersdk.ErrScheduledPlanInvalid) {
			httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusBadRequest, Code: "scheduler.plan_invalid", Cause: err})
			return
		}
		if errors.Is(err, schedulersdk.ErrScheduledPlanConflict) {
			httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusConflict, Code: "scheduler.plan_conflict", Cause: err})
			return
		}
		if errors.Is(err, schedulersdk.ErrScheduledPlanNotFound) {
			httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusNotFound, Code: "scheduler.plan_not_found", Cause: err})
			return
		}
		if errors.Is(err, schedulersdk.ErrDefinitionPublicationRequired) {
			httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusBadRequest, Code: "scheduler.definition_publication_required", Cause: err})
			return
		}
		if errors.Is(err, schedulersdk.ErrDefinitionPublicationSessionMismatch) || errors.Is(err, schedulersdk.ErrDefinitionSnapshotStale) || errors.Is(err, schedulersdk.ErrDefinitionSnapshotConflict) {
			httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusConflict, Code: "scheduler.definition_publication_rejected", Cause: err})
			return
		}
		httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusBadGateway, Code: "scheduler.request_failed", Cause: err})
		return
	}
	if value == nil {
		httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusMethodNotAllowed, Code: "backend.method_not_allowed"})
		return
	}
	httpresponse.WriteJSON(response, http.StatusOK, value)
}

func schedulerRoutePattern(method, path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 4 || len(parts) > 6 || parts[0] != "v1" || parts[1] != "applications" || strings.TrimSpace(parts[2]) == "" {
		return ""
	}
	base := "/v1/applications/{runtime_id}/"
	switch parts[3] {
	case "descriptor":
		if len(parts) == 4 && method == http.MethodGet {
			return "GET " + base + "descriptor"
		}
	case "definitions":
		if len(parts) == 4 && method == http.MethodPut {
			return "PUT " + base + "definitions"
		}
		if len(parts) == 6 && parts[5] == "reschedule" && method == http.MethodPost {
			return "POST " + base + "definitions/{definition_id}/reschedule"
		}
	case "plans":
		if len(parts) == 4 && (method == http.MethodGet || method == http.MethodPost) {
			return method + " " + base + "plans"
		}
		if len(parts) == 5 && (method == http.MethodGet || method == http.MethodPut || method == http.MethodDelete) {
			return method + " " + base + "plans/{plan_id}"
		}
		if len(parts) == 6 && parts[5] == "deletion-receipt" && method == http.MethodGet {
			return "GET " + base + "plans/{plan_id}/deletion-receipt"
		}
		if len(parts) == 6 && (parts[5] == "pause" || parts[5] == "resume") && method == http.MethodPost {
			return "POST " + base + "plans/{plan_id}/" + parts[5]
		}
	case "definition-publication-sessions":
		if len(parts) == 4 && method == http.MethodPost {
			return "POST " + base + "definition-publication-sessions"
		}
	case "preview", "ticks", "triggers":
		if len(parts) == 4 && method == http.MethodPost {
			return "POST " + base + parts[3]
		}
	case "runs":
		if len(parts) == 4 && method == http.MethodGet {
			return "GET " + base + "runs"
		}
		if len(parts) == 5 && method == http.MethodGet {
			return "GET " + base + "runs/{run_id}"
		}
		if len(parts) == 6 && (parts[5] == "retry" || parts[5] == "cancel") && method == http.MethodPost {
			return "POST " + base + "runs/{run_id}/" + parts[5]
		}
	case "dead-letters":
		if len(parts) == 4 && method == http.MethodGet {
			return "GET " + base + "dead-letters"
		}
		if len(parts) == 5 && method == http.MethodGet {
			return "GET " + base + "dead-letters/{dead_letter_id}"
		}
		if len(parts) == 6 && (parts[5] == "resolve" || parts[5] == "requeue") && method == http.MethodPost {
			return "POST " + base + "dead-letters/{dead_letter_id}/" + parts[5]
		}
	case "binding":
		if len(parts) == 4 && method == http.MethodDelete {
			return "DELETE " + base + "binding"
		}
	}
	return ""
}

func (s *Server) authenticate(request *http.Request) (schedulersdk.ApplicationRef, bool) {
	const prefix = "Bearer "
	authorization := request.Header.Get("Authorization")
	if !strings.HasPrefix(authorization, prefix) {
		return schedulersdk.ApplicationRef{}, false
	}
	presented := strings.TrimPrefix(authorization, prefix)
	if presented == "" {
		return schedulersdk.ApplicationRef{}, false
	}
	presentedDigest := sha256.Sum256([]byte(presented))
	matched := -1
	for index, credential := range s.credentials {
		if subtle.ConstantTimeCompare(presentedDigest[:], credential.tokenDigest[:]) == 1 {
			matched = index
		}
	}
	if matched < 0 {
		return schedulersdk.ApplicationRef{}, false
	}
	return s.credentials[matched].application, true
}

func decode(response http.ResponseWriter, request *http.Request, target any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusBadRequest, Code: "scheduler.request_invalid", Cause: err})
		return false
	}
	return true
}
