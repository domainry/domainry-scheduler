package remote

import (
	"context"

	sdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/saashost"
)

func (b *binding) ReadScheduledPlanDeletion(ctx context.Context, lookup sdk.ScheduledPlanLookup) (sdk.ScheduledPlanDeleteReceipt, error) {
	reader, ok := b.transport.(saashost.ScheduledPlanDeletionTransport)
	if !ok || !b.descriptor.Supports(sdk.CapabilityScheduledPlanDeletionRead) {
		return sdk.ScheduledPlanDeleteReceipt{}, sdk.ErrScheduledPlanDeletionReadUnsupported
	}
	return reader.ReadScheduledPlanDeletion(ctx, b.application, lookup)
}

var _ sdk.ScheduledPlanDeletionReader = (*binding)(nil)
