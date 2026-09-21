package schedulersdkadapter

import (
	"context"
	"testing"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/dispatchgateway"
)

type gatewayStub struct {
	request dispatchgateway.Request
}

func (g *gatewayStub) Dispatch(_ context.Context, _ schedulersdk.ApplicationRef, request dispatchgateway.Request) (dispatchgateway.Receipt, error) {
	g.request = request
	return dispatchgateway.Receipt{ExecutionID: request.ExecutionID, ID: "receipt-1", Owner: "workflow", Status: "accepted"}, nil
}

func TestRemoteDownstreamsDispatchesThroughRuntimeGateway(t *testing.T) {
	gateway := &gatewayStub{}
	factory := RemoteDownstreams(gateway)
	host, err := factory(t.Context(), schedulersdk.ApplicationRef{RuntimeID: "runtime-a"})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := host.Dispatch(t.Context(), schedulersdk.Trigger{RunID: "run-1", DefinitionKey: "definition-1", IdempotencyKey: "run-1"})
	if err != nil || receipt.ID != "receipt-1" || gateway.request.RuntimeID != "runtime-a" || gateway.request.ExecutionID != "run-1" || gateway.request.DefinitionKey != "definition-1" {
		t.Fatalf("receipt=%#v request=%#v err=%v", receipt, gateway.request, err)
	}
}
