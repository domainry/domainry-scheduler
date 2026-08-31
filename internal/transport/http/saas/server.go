package saas

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
)

type Service interface {
	Descriptor(context.Context, schedulersdk.ApplicationRef) (schedulersdk.Descriptor, error)
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
	BearerToken string
	Service     Service
}

type Server struct {
	token   string
	service Service
}

func New(options Options) *Server {
	return &Server{token: strings.TrimSpace(options.BearerToken), service: options.Service}
}
func (s *Server) Routes() http.Handler { return s }

func (s *Server) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if s.service == nil {
		http.Error(response, "Scheduler service unavailable", http.StatusServiceUnavailable)
		return
	}
	if s.token != "" && request.Header.Get("Authorization") != "Bearer "+s.token {
		http.Error(response, "unauthorized", http.StatusUnauthorized)
		return
	}
	parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
	if len(parts) < 4 || len(parts) > 6 || parts[0] != "v1" || parts[1] != "applications" {
		http.NotFound(response, request)
		return
	}
	application := schedulersdk.ApplicationRef{RuntimeID: parts[2]}
	if application.Validate() != nil {
		http.Error(response, "invalid application", http.StatusBadRequest)
		return
	}
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
		if request.Method != http.MethodDelete {
			break
		}
		err = s.service.Close(request.Context(), application)
		if err == nil {
			response.WriteHeader(http.StatusNoContent)
			return
		}
	default:
		http.NotFound(response, request)
		return
	}
	if err != nil {
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

func decode(response http.ResponseWriter, request *http.Request, target any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		http.Error(response, "invalid Scheduler request", http.StatusBadRequest)
		return false
	}
	return true
}
