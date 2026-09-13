package remote

import (
	"context"
	"errors"
	"testing"

	sdk "github.com/domainry/domainry-scheduler-sdk"
)

type deletionTransportStub struct {
	transportStub
	app    sdk.ApplicationRef
	lookup sdk.ScheduledPlanLookup
	err    error
	calls  int
}

func (s *deletionTransportStub) ReadScheduledPlanDeletion(_ context.Context, app sdk.ApplicationRef, lookup sdk.ScheduledPlanLookup) (sdk.ScheduledPlanDeleteReceipt, error) {
	s.app, s.lookup = app, lookup
	s.calls++
	return sdk.ScheduledPlanDeleteReceipt{PlanID: lookup.PlanID, Revision: 2, Deleted: true}, s.err
}

func TestRemoteDeletionReadPreservesScopeAndRequiresAdvertisedCapability(t *testing.T) {
	transport := &deletionTransportStub{}
	b := &binding{transport: transport, application: sdk.ApplicationRef{RuntimeID: "bound-runtime"}}
	lookup := sdk.ScheduledPlanLookup{PlanID: "plan", Owner: sdk.ScheduledPlanOwner{WorkspaceID: "workspace", UserID: "reader", ProductKey: "agent"}}
	if _, err := b.ReadScheduledPlanDeletion(t.Context(), lookup); !errors.Is(err, sdk.ErrScheduledPlanDeletionReadUnsupported) || transport.calls != 0 {
		t.Fatal(err)
	}
	b.descriptor.Capabilities = []string{sdk.CapabilityScheduledPlanDeletionRead}
	got, err := b.ReadScheduledPlanDeletion(t.Context(), lookup)
	if err != nil || !got.Deleted || transport.app != b.application || transport.lookup != lookup || transport.calls != 1 {
		t.Fatal(got, err, transport)
	}
	transport.err = sdk.ErrScheduledPlanNotFound
	if _, err := b.ReadScheduledPlanDeletion(t.Context(), lookup); !errors.Is(err, sdk.ErrScheduledPlanNotFound) {
		t.Fatal(err)
	}
	b.transport = &transportStub{}
	if _, err := b.ReadScheduledPlanDeletion(t.Context(), lookup); !errors.Is(err, sdk.ErrScheduledPlanDeletionReadUnsupported) {
		t.Fatal(err)
	}
}
