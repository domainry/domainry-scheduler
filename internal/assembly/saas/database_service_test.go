package saas

import (
	"context"
	"database/sql"
	"testing"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	_ "modernc.org/sqlite"
)

type databaseDownstream struct{}

func (databaseDownstream) Dispatch(_ context.Context, trigger schedulersdk.Trigger) (schedulersdk.DownstreamReceipt, error) {
	return schedulersdk.DownstreamReceipt{ID: "receipt-" + trigger.RunID, Owner: trigger.Target.Owner, Status: "accepted"}, nil
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
	definition := schedulersdk.Definition{Key: "daily", Status: "enabled", Revision: "v1", Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60}, Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "workflow", Operation: "run"}}
	if err := service.Reconcile(requestCtx, ref, schedulersdk.DefinitionSnapshot{Revision: 1, Definitions: []schedulersdk.Definition{definition}}); err != nil {
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
func (databaseDownstream) ResolveHTTPConnection(context.Context, string) (modulehost.HTTPConnection, error) {
	return modulehost.HTTPConnection{}, nil
}

func TestDatabaseServicePersistsAndIsolatesSaaSApplications(t *testing.T) {
	db, err := sql.Open("sqlite", "file:scheduler-saas?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, err := NewDatabaseService(DatabaseServiceOptions{Database: db, Driver: "sqlite", WorkerID: "scheduler-saas-a", Downstreams: func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error) {
		return databaseDownstream{}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	definition := schedulersdk.Definition{Key: "daily", Name: "Daily", Status: "enabled", Revision: "v1", Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60}, Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "workflow", Operation: "run"}}
	for _, runtimeID := range []string{"runtime-a", "runtime-b"} {
		ref := schedulersdk.ApplicationRef{RuntimeID: runtimeID}
		if err := service.Reconcile(t.Context(), ref, schedulersdk.DefinitionSnapshot{Revision: 1, Definitions: []schedulersdk.Definition{definition}}); err != nil {
			t.Fatal(err)
		}
		run, err := service.TriggerNow(t.Context(), ref, "daily", "test")
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != "leased" {
			t.Fatalf("trigger response status=%s", run.Status)
		}
		runs, err := service.Runs(t.Context(), ref, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(runs) != 1 || runs[0].Status != "succeeded" || runs[0].DownstreamReceipt.ID == "" {
			t.Fatalf("%s runs=%+v", runtimeID, runs)
		}
	}
	var count int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM "_scheduler_runs"`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("scheduler SaaS run rows=%d", count)
	}
}
