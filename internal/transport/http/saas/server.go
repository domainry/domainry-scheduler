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

	"github.com/domainry/domainry-foundation/modulecapability"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	capability "github.com/domainry/domainry-scheduler/internal/capability"
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
	capability  http.Handler
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
	binding, err := capability.NewBinding()
	if err != nil {
		return nil, err
	}
	handler, err := modulecapability.NewHTTPHandler(binding, func(*http.Request) error { return nil })
	if err != nil {
		return nil, err
	}
	return &Server{credentials: credentials, service: options.Service, capability: handler}, nil
}
func (s *Server) Routes() http.Handler { return s }

func (s *Server) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	// The source-owned Action adapter is safe in Module mode because Runtime's
	// listener applies the Action permission and operation guards. Standalone
	// SaaS has no equivalent trusted human principal contract, so it must not
	// mount the /scheduler surface behind a machine credential.
	if request.URL.Path == "/scheduler" || strings.HasPrefix(request.URL.Path, "/scheduler/") {
		http.NotFound(response, request)
		return
	}
	authenticated, ok := s.authenticate(request)
	if !ok {
		http.Error(response, "unauthorized", http.StatusUnauthorized)
		return
	}
	if strings.HasPrefix(request.URL.Path, modulecapability.HTTPPrefix) {
		s.capability.ServeHTTP(response, request)
		return
	}
	parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
	if len(parts) < 4 || len(parts) > 6 || parts[0] != "v1" || parts[1] != "applications" {
		http.NotFound(response, request)
		return
	}
	requested := schedulersdk.ApplicationRef{RuntimeID: strings.TrimSpace(parts[2])}
	if requested.Validate() != nil {
		http.Error(response, "invalid application", http.StatusBadRequest)
		return
	}
	if requested.RuntimeID != authenticated.RuntimeID {
		http.Error(response, "forbidden", http.StatusForbidden)
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
		http.NotFound(response, request)
		return
	}
	if err != nil {
		if errors.Is(err, schedulersdk.ErrDefinitionPublicationRequired) {
			http.Error(response, "Scheduler definition publisher session required", http.StatusBadRequest)
			return
		}
		if errors.Is(err, schedulersdk.ErrDefinitionPublicationSessionMismatch) || errors.Is(err, schedulersdk.ErrDefinitionSnapshotStale) || errors.Is(err, schedulersdk.ErrDefinitionSnapshotConflict) {
			http.Error(response, "Scheduler definition publication rejected", http.StatusConflict)
			return
		}
		http.Error(response, "Scheduler request failed", http.StatusBadGateway)
		return
	}
	if value == nil {
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	response.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(response).Encode(value)
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
		http.Error(response, "invalid Scheduler request", http.StatusBadRequest)
		return false
	}
	return true
}
