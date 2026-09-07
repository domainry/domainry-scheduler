package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
)

type connectionProvider struct{ connection modulehost.HTTPConnection }

func (p connectionProvider) ResolveHTTPConnection(context.Context, string) (modulehost.HTTPConnection, error) {
	return p.connection, nil
}

type handlerClient struct{ handler http.Handler }

func (c handlerClient) Do(request *http.Request) (*http.Response, error) {
	response := httptest.NewRecorder()
	c.handler.ServeHTTP(response, request)
	return response.Result(), nil
}

func TestHTTPExecutorSignsAndPreservesTriggerEvidence(t *testing.T) {
	client := handlerClient{handler: http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Idempotency-Key") != "scheduler:daily:window" || request.Header.Get("X-Domainry-Schedule-Window") != "window" || request.Header.Get("X-Signature") == "" {
			t.Errorf("missing trigger headers: %#v", request.Header)
		}
		if request.Header.Get("Authorization") != "" {
			t.Error("unsafe configured authorization header was accepted")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"receipt_id": "downstream-1", "status": "accepted"})
	})}
	executor := New(connectionProvider{modulehost.HTTPConnection{BaseURL: "http://downstream.test", ClientID: "scheduler", Secret: []byte("secret"), Auth: "hmac_v1", Operations: map[string]modulehost.HTTPOperation{"sync": {Path: "/sync", Method: "POST", Timeout: time.Second, Headers: map[string]string{"Authorization": "forbidden"}}}}}, client)
	receipt, err := executor.Dispatch(t.Context(), schedulersdk.Trigger{RunID: "run-1", WindowKey: "window", IdempotencyKey: "scheduler:daily:window", Target: schedulersdk.TargetRef{Type: "http", ConnectionKey: "service", Operation: "sync", Payload: json.RawMessage(`{"mode":"daily"}`)}})
	if err != nil || receipt.ID != "downstream-1" {
		t.Fatalf("receipt=%#v err=%v", receipt, err)
	}
}

func TestHTTPExecutorOrdinaryFractionalRunUsesLegacyWindowInHeaderAndHMAC(t *testing.T) {
	secret := []byte("ordinary-secret")
	clientID := "scheduler"
	payload := json.RawMessage(`{"mode":"fractional"}`)
	scheduledFor := time.Date(2026, 9, 7, 1, 30, 15, 987654321, time.UTC)
	windowKey := scheduledFor.Format(time.RFC3339)
	client := handlerClient{handler: http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		timestamp := request.Header.Get("X-Timestamp")
		if got := request.Header.Get("X-Domainry-Schedule-Window"); got != windowKey {
			t.Errorf("schedule window=%q want=%q", got, windowKey)
		}
		if got, want := request.Header.Get("X-Signature"), signature(secret, clientID, timestamp, windowKey, payload); got != want {
			t.Errorf("signature=%q want=%q", got, want)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"receipt_id": "downstream-fractional", "status": "accepted"})
	})}
	executor := New(connectionProvider{modulehost.HTTPConnection{BaseURL: "http://downstream.test", ClientID: clientID, Secret: secret, Auth: "hmac_v1", Operations: map[string]modulehost.HTTPOperation{"sync": {Path: "/sync", Method: "POST"}}}}, client)
	trigger := schedulersdk.Trigger{RunID: "run-fractional", ScheduledFor: scheduledFor, WindowKey: windowKey, IdempotencyKey: "scheduler:fractional:window", Target: schedulersdk.TargetRef{Type: "http", ConnectionKey: "service", Operation: "sync", Payload: payload}}
	receipt, err := executor.Dispatch(t.Context(), trigger)
	if err != nil || receipt.ID != "downstream-fractional" {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
}

func TestHTTPExecutorClassifiesRetryableStatusAndRejectsAbsoluteOperation(t *testing.T) {
	client := handlerClient{handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) })}
	provider := connectionProvider{modulehost.HTTPConnection{BaseURL: "http://downstream.test", Operations: map[string]modulehost.HTTPOperation{"sync": {Path: "/sync"}}}}
	_, err := New(provider, client).Dispatch(t.Context(), schedulersdk.Trigger{Target: schedulersdk.TargetRef{ConnectionKey: "service", Operation: "sync"}})
	classified, ok := err.(*ExecutionError)
	if !ok || !classified.Retryable {
		t.Fatalf("error=%#v", err)
	}
	provider.connection.Operations["sync"] = modulehost.HTTPOperation{Path: "https://attacker.example/sync"}
	if _, err := New(provider, client).Dispatch(t.Context(), schedulersdk.Trigger{Target: schedulersdk.TargetRef{ConnectionKey: "service", Operation: "sync"}}); err == nil {
		t.Fatal("absolute operation URL accepted")
	}
}

func TestHTTPExecutorRejectsBusinessFailureInSuccessfulResponse(t *testing.T) {
	client := handlerClient{handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"errCode":4001,"errMsg":"task rejected"}`))
	})}
	provider := connectionProvider{modulehost.HTTPConnection{BaseURL: "http://downstream.test", Operations: map[string]modulehost.HTTPOperation{"sync": {Path: "/sync"}}}}
	_, err := New(provider, client).Dispatch(t.Context(), schedulersdk.Trigger{Target: schedulersdk.TargetRef{ConnectionKey: "service", Operation: "sync"}})
	classified, ok := err.(*ExecutionError)
	if !ok || classified.StatusCode != http.StatusOK || classified.Retryable {
		t.Fatalf("unexpected business error classification: %#v", err)
	}
}
