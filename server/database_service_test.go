package server

import (
	"context"
	"database/sql"
	"testing"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	_ "modernc.org/sqlite"
)

type databaseDownstream struct{}

func (databaseDownstream) Dispatch(_ context.Context, trigger schedulersdk.Trigger) (schedulersdk.DownstreamReceipt, error) {
	return schedulersdk.DownstreamReceipt{ID: "receipt-" + trigger.RunID, Owner: trigger.Target.Owner, Status: "accepted"}, nil
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
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM "scheduler_runs"`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("scheduler SaaS run rows=%d", count)
	}
}
