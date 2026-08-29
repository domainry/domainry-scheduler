package module

import (
	"context"
	"fmt"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
)

type Options struct{}

func OptionsFromEnvironment() Options { return Options{} }

type Factory struct{ options Options }

func NewFactory(options Options) *Factory { return &Factory{options: options} }

func (*Factory) Open(context.Context, schedulersdk.ApplicationRef) (schedulersdk.Binding, error) {
	return nil, fmt.Errorf("Scheduler Module host is required")
}

func (*Factory) OpenModule(ctx context.Context, application schedulersdk.ApplicationRef, host modulehost.Host) (schedulersdk.Binding, error) {
	if err := application.Validate(); err != nil {
		return nil, err
	}
	if host == nil || host.Definitions() == nil || host.Runs() == nil || host.Dispatcher() == nil || host.HTTPConnections() == nil {
		return nil, fmt.Errorf("Scheduler host is incomplete")
	}
	ownerCtx, cancel := context.WithCancel(ctx)
	return newBinding(ownerCtx, cancel, application, host), nil
}

var _ schedulersdk.Factory = (*Factory)(nil)
var _ modulehost.Factory = (*Factory)(nil)
