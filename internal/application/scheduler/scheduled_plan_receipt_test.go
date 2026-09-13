package scheduler

import (
	"context"
	"errors"
	"testing"

	sdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/persistence"
)

func TestReadScheduledPlanDeletionUsesOnlyOwnedTombstone(t *testing.T) {
	owner := sdk.ScheduledPlanOwner{WorkspaceID: "workspace", UserID: "user", ProductKey: "agent"}
	repo := &scheduledPlanRepositoryStub{record: persistence.ScheduledPlanRecord{Plan: sdk.ScheduledPlan{ID: "plan", Owner: owner, Status: sdk.ScheduledPlanStatusDeleted, Revision: 4}}}
	// No clock, definition repository, dispatcher or projection host is installed.
	service := &Service{planRepository: repo}
	lookup := sdk.ScheduledPlanLookup{Owner: owner, PlanID: "plan"}
	got, err := service.ReadScheduledPlanDeletion(t.Context(), lookup)
	if err != nil || got != (sdk.ScheduledPlanDeleteReceipt{PlanID: "plan", Revision: 4, Deleted: true}) {
		t.Fatal(got, err)
	}
	for _, field := range []string{"user", "workspace", "product", "id", "live", "invalid-revision"} {
		t.Run(field, func(t *testing.T) {
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
			case "live":
				repo.record.Plan.Status = sdk.ScheduledPlanStatusEnabled
			case "invalid-revision":
				repo.record.Plan.Revision = 1
			}
			if _, err := service.ReadScheduledPlanDeletion(t.Context(), changed); !errors.Is(err, sdk.ErrScheduledPlanNotFound) {
				t.Fatal(err)
			}
			repo.record.Plan.Status, repo.record.Plan.Revision = sdk.ScheduledPlanStatusDeleted, 4
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := service.ReadScheduledPlanDeletion(ctx, lookup); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := service.GetScheduledPlan(t.Context(), lookup); !errors.Is(err, sdk.ErrScheduledPlanNotFound) {
		t.Fatal("public get disclosed tombstone", err)
	}
}
