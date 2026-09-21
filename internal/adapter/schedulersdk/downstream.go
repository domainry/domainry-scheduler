package schedulersdkadapter

import (
	"context"
	"fmt"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/dispatchgateway"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
)

type DownstreamHost interface {
	modulehost.Dispatcher
	modulehost.HTTPConnectionProvider
}

// RemoteDownstreams adapts the authenticated Scheduler-to-Runtime callback
// protocol for standalone SaaS deployments. Runtime-owned operations and
// runtime_callback HTTP targets are both executed behind the Runtime boundary.
func RemoteDownstreams(gateway dispatchgateway.Gateway) func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error) {
	return RemoteDownstreamsByApplication(func(context.Context, schedulersdk.ApplicationRef) (dispatchgateway.Gateway, error) {
		return gateway, nil
	})
}

// RemoteDownstreamsByApplication resolves a callback endpoint per Runtime,
// which keeps a multi-tenant Scheduler SaaS from sharing callback credentials
// or routing one application's trigger into another Runtime.
func RemoteDownstreamsByApplication(resolve func(context.Context, schedulersdk.ApplicationRef) (dispatchgateway.Gateway, error)) func(context.Context, schedulersdk.ApplicationRef) (DownstreamHost, error) {
	return func(ctx context.Context, application schedulersdk.ApplicationRef) (DownstreamHost, error) {
		if resolve == nil {
			return nil, fmt.Errorf("Scheduler Dispatch Gateway is unavailable")
		}
		if err := application.Validate(); err != nil {
			return nil, err
		}
		gateway, err := resolve(ctx, application)
		if err != nil {
			return nil, err
		}
		if gateway == nil {
			return nil, fmt.Errorf("Scheduler Dispatch Gateway is unavailable")
		}
		return remoteDownstream{application: application, gateway: gateway}, nil
	}
}

type remoteDownstream struct {
	application schedulersdk.ApplicationRef
	gateway     dispatchgateway.Gateway
}

func (d remoteDownstream) Dispatch(ctx context.Context, trigger schedulersdk.Trigger) (schedulersdk.DownstreamReceipt, error) {
	receipt, err := d.gateway.Dispatch(ctx, d.application, dispatchgateway.Request{
		RuntimeID: d.application.RuntimeID, ExecutionID: trigger.RunID, DefinitionKey: trigger.DefinitionKey, IdempotencyKey: trigger.IdempotencyKey,
		DueAt: trigger.ScheduledFor, Target: trigger.Target,
	})
	if err != nil {
		return schedulersdk.DownstreamReceipt{}, err
	}
	return schedulersdk.DownstreamReceipt{ID: receipt.ID, Owner: receipt.Owner, Status: receipt.Status}, nil
}

func (remoteDownstream) ResolveHTTPConnection(context.Context, string) (modulehost.HTTPConnection, error) {
	return modulehost.HTTPConnection{}, fmt.Errorf("Scheduler SaaS direct HTTP requires a service-owned connection provider; runtime_callback targets use the Dispatch Gateway")
}

var _ DownstreamHost = remoteDownstream{}
