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

func TestHTTPExecutorSignsAndPreservesTriggerEvidence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Idempotency-Key") != "scheduler:daily:window" || request.Header.Get("X-Domainry-Schedule-Window") != "window" || request.Header.Get("X-Signature") == "" {
			t.Errorf("missing trigger headers: %#v", request.Header)
		}
		if request.Header.Get("Authorization") != "" {
			t.Error("unsafe configured authorization header was accepted")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"receipt_id": "downstream-1", "status": "accepted"})
	}))
	defer server.Close()
	executor := New(connectionProvider{modulehost.HTTPConnection{BaseURL: server.URL, ClientID: "scheduler", Secret: []byte("secret"), Auth: "hmac_v1", Operations: map[string]modulehost.HTTPOperation{"sync": {Path: "/sync", Method: "POST", Timeout: time.Second, Headers: map[string]string{"Authorization": "forbidden"}}}}}, server.Client())
	receipt, err := executor.Dispatch(t.Context(), schedulersdk.Trigger{RunID: "run-1", WindowKey: "window", IdempotencyKey: "scheduler:daily:window", Target: schedulersdk.TargetRef{Type: "http", ConnectionKey: "service", Operation: "sync", Payload: json.RawMessage(`{"mode":"daily"}`)}})
	if err != nil || receipt.ID != "downstream-1" {
		t.Fatalf("receipt=%#v err=%v", receipt, err)
	}
}

func TestHTTPExecutorClassifiesRetryableStatusAndRejectsAbsoluteOperation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer server.Close()
	provider := connectionProvider{modulehost.HTTPConnection{BaseURL: server.URL, Operations: map[string]modulehost.HTTPOperation{"sync": {Path: "/sync"}}}}
	_, err := New(provider, server.Client()).Dispatch(t.Context(), schedulersdk.Trigger{Target: schedulersdk.TargetRef{ConnectionKey: "service", Operation: "sync"}})
	classified, ok := err.(*ExecutionError)
	if !ok || !classified.Retryable {
		t.Fatalf("error=%#v", err)
	}
	provider.connection.Operations["sync"] = modulehost.HTTPOperation{Path: "https://attacker.example/sync"}
	if _, err := New(provider, server.Client()).Dispatch(t.Context(), schedulersdk.Trigger{Target: schedulersdk.TargetRef{ConnectionKey: "service", Operation: "sync"}}); err == nil {
		t.Fatal("absolute operation URL accepted")
	}
}

func TestHTTPExecutorRejectsBusinessFailureInSuccessfulResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"errCode":4001,"errMsg":"task rejected"}`))
	}))
	defer server.Close()
	provider := connectionProvider{modulehost.HTTPConnection{BaseURL: server.URL, Operations: map[string]modulehost.HTTPOperation{"sync": {Path: "/sync"}}}}
	_, err := New(provider, server.Client()).Dispatch(t.Context(), schedulersdk.Trigger{Target: schedulersdk.TargetRef{ConnectionKey: "service", Operation: "sync"}})
	classified, ok := err.(*ExecutionError)
	if !ok || classified.StatusCode != http.StatusOK || classified.Retryable {
		t.Fatalf("unexpected business error classification: %#v", err)
	}
}
