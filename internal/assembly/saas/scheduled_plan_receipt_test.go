package saas

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	sdk "github.com/domainry/domainry-scheduler-sdk"
	transport "github.com/domainry/domainry-scheduler-sdk/saashost/httptransport"
	capability "github.com/domainry/domainry-scheduler/capability"
	server "github.com/domainry/domainry-scheduler/internal/transport/http/saas"
)

func TestDeletionReceiptHTTPReadsDurableTombstoneAfterRestart(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-deletion-receipts?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	app := sdk.ApplicationRef{RuntimeID: "runtime-receipt"}
	open := func() (*DatabaseService, *transport.Transport) {
		s, err := NewDatabaseService(DatabaseServiceOptions{Context: t.Context(), Database: db, Driver: "sqlite", WorkerID: "receipt-owner", Worker: sdk.WorkerConfig{Enabled: false}, Applications: []sdk.ApplicationRef{app}, Downstreams: func(context.Context, sdk.ApplicationRef) (DownstreamHost, error) { return databaseDownstream{}, nil }})
		if err != nil {
			t.Fatal(err)
		}
		h, err := server.New(server.Options{ApplicationTokens: map[string]string{app.RuntimeID: "receipt-secret"}, Service: s})
		if err != nil {
			t.Fatal(err)
		}
		binding, err := capability.Open(capability.Inputs{})
		if err != nil {
			t.Fatal(err)
		}
		summary, err := binding.CapabilitySummary(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		client, err := transport.Open(t.Context(), transport.Config{Endpoint: "https://scheduler.test", Token: "receipt-secret", Client: &http.Client{Transport: databaseHTTPRoundTripper{handler: h}}, CapabilityContractSHA256: summary.Identity.ContractSHA256})
		if err != nil {
			t.Fatal(err)
		}
		return s, client
	}
	first, client := open()
	owner := sdk.ScheduledPlanOwner{WorkspaceID: "workspace", UserID: "user", ProductKey: "agent"}
	created, err := client.CreateScheduledPlan(t.Context(), app, sdk.ScheduledPlanCreate{ClientID: "receipt", Name: "原计划", Owner: owner, Timezone: "Asia/Shanghai", Trigger: sdk.ScheduledPlanTrigger{Type: "recurring", Schedule: &sdk.Schedule{Type: "daily_at", TimeOfDay: "09:00", Timezone: "Asia/Shanghai"}}, Input: json.RawMessage(`{"goal":"原始内容"}`), AllowedActions: []string{"todo.list"}, Target: sdk.TargetRef{Owner: "agent", Operation: "conversation_task_start"}})
	if err != nil {
		t.Fatal(err)
	}
	lookup := sdk.ScheduledPlanLookup{Owner: owner, PlanID: created.Plan.ID}
	if _, err := client.ReadScheduledPlanDeletion(t.Context(), app, lookup); !errors.Is(err, sdk.ErrScheduledPlanNotFound) {
		t.Fatal("live plan treated as deleted", err)
	}
	deleted, err := client.DeleteScheduledPlan(t.Context(), app, sdk.ScheduledPlanStatusChange{Owner: owner, PlanID: created.Plan.ID, ExpectedRevision: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	second, reader := open()
	defer second.Shutdown(t.Context())
	for range 2 {
		got, err := reader.ReadScheduledPlanDeletion(t.Context(), app, lookup)
		if err != nil || got != deleted {
			t.Fatal(got, deleted, err)
		}
	}
	for _, field := range []string{"user", "workspace", "product", "id"} {
		changed := lookup
		switch field {
		case "user":
			changed.Owner.UserID = "other"
		case "workspace":
			changed.Owner.WorkspaceID = "other"
		case "product":
			changed.Owner.ProductKey = "other"
		case "id":
			changed.PlanID = "missing"
		}
		if _, err := reader.ReadScheduledPlanDeletion(t.Context(), app, changed); !errors.Is(err, sdk.ErrScheduledPlanNotFound) {
			t.Fatal(field, err)
		}
	}
	if _, err := reader.GetScheduledPlan(t.Context(), app, lookup); !errors.Is(err, sdk.ErrScheduledPlanNotFound) {
		t.Fatal("public get exposed deleted plan", err)
	}
	if _, err := reader.ReadScheduledPlanDeletion(t.Context(), sdk.ApplicationRef{RuntimeID: "another-runtime"}, lookup); err == nil {
		t.Fatal("cross-runtime receipt leaked")
	}
}

func TestDeletionReceiptDoesNotInitializeAnApplication(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-lazy-deletion-reader?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	downstreamLookups := 0
	service, err := NewDatabaseService(DatabaseServiceOptions{Context: t.Context(), Database: db, Driver: "sqlite", WorkerID: "lazy-receipt", Worker: sdk.WorkerConfig{Enabled: true}, Downstreams: func(context.Context, sdk.ApplicationRef) (DownstreamHost, error) {
		downstreamLookups++
		return nil, errors.New("read must not initialize application infrastructure")
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Shutdown(t.Context())
	_, err = service.ReadScheduledPlanDeletion(t.Context(), sdk.ApplicationRef{RuntimeID: "not-initialized"}, sdk.ScheduledPlanLookup{Owner: sdk.ScheduledPlanOwner{WorkspaceID: "workspace", UserID: "user", ProductKey: "agent"}, PlanID: "plan"})
	if err == nil || downstreamLookups != 0 || len(service.applications) != 0 {
		t.Fatal("receipt reading invoked lazy application startup", err, downstreamLookups)
	}
}
