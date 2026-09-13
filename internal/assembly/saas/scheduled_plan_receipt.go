package saas

import (
	"context"
	"fmt"
	"strings"

	sdk "github.com/domainry/domainry-scheduler-sdk"
)

func (s *DatabaseService) ReadScheduledPlanDeletion(ctx context.Context, ref sdk.ApplicationRef, lookup sdk.ScheduledPlanLookup) (sdk.ScheduledPlanDeleteReceipt, error) {
	if err := ctx.Err(); err != nil {
		return sdk.ScheduledPlanDeleteReceipt{}, err
	}
	if err := ref.Validate(); err != nil {
		return sdk.ScheduledPlanDeleteReceipt{}, err
	}
	// The normal application resolver can hydrate definitions and start a
	// worker. Receipt reads use only a binding initialized by host startup or
	// the explicit binding handshake, never that effectful lazy resolver.
	key := strings.TrimSpace(ref.RuntimeID)
	s.mu.Lock()
	app := s.applications[key]
	allowed := len(s.configured) == 0 || s.configured[key]
	s.mu.Unlock()
	if !allowed || app == nil {
		return sdk.ScheduledPlanDeleteReceipt{}, fmt.Errorf("Scheduler application is unavailable for receipt reading")
	}
	reader, ok := app.binding.(sdk.ScheduledPlanDeletionReader)
	if !ok {
		return sdk.ScheduledPlanDeleteReceipt{}, sdk.ErrScheduledPlanDeletionReadUnsupported
	}
	return reader.ReadScheduledPlanDeletion(ctx, lookup)
}
