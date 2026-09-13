package scheduler

import (
	"context"
	"strings"

	sdk "github.com/domainry/domainry-scheduler-sdk"
)

func (b *Service) ReadScheduledPlanDeletion(ctx context.Context, lookup sdk.ScheduledPlanLookup) (sdk.ScheduledPlanDeleteReceipt, error) {
	if err := ctx.Err(); err != nil {
		return sdk.ScheduledPlanDeleteReceipt{}, err
	}
	if b == nil || b.planRepository == nil {
		return sdk.ScheduledPlanDeleteReceipt{}, sdk.ErrScheduledPlanDeletionReadUnsupported
	}
	lookup.Owner = normalizeScheduledPlanOwner(lookup.Owner)
	lookup.PlanID = strings.TrimSpace(lookup.PlanID)
	if lookup.Owner.Validate() != nil || !scheduledPlanManagementKey(lookup.PlanID) {
		return sdk.ScheduledPlanDeleteReceipt{}, sdk.ErrScheduledPlanInvalid
	}
	plan, err := b.planRepository.GetScheduledPlan(ctx, lookup)
	if err != nil {
		return sdk.ScheduledPlanDeleteReceipt{}, err
	}
	if plan.ID != lookup.PlanID || plan.Owner != lookup.Owner || plan.Status != sdk.ScheduledPlanStatusDeleted || plan.Revision < 2 {
		return sdk.ScheduledPlanDeleteReceipt{}, sdk.ErrScheduledPlanNotFound
	}
	return sdk.ScheduledPlanDeleteReceipt{PlanID: plan.ID, Revision: plan.Revision, Deleted: true}, ctx.Err()
}

var _ sdk.ScheduledPlanDeletionReader = (*Service)(nil)
