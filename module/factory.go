package module

import (
	"context"
	"fmt"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	moduleassembly "github.com/domainry/domainry-scheduler/internal/assembly/module"
)

type Options struct{}

func OptionsFromEnvironment() Options { return Options{} }

type Factory struct{ options Options }

// SaaSHost is retained for callers that assemble a Scheduler-owned database
// directly. The implementation is shared with the public server facade through
// the internal hosted composition boundary.
type SaaSHost = moduleassembly.SaaSHost

func NewFactory(options Options) *Factory { return &Factory{options: options} }

func (*Factory) Open(context.Context, schedulersdk.ApplicationRef) (schedulersdk.Binding, error) {
	return nil, fmt.Errorf("Scheduler Module host is required")
}

func (*Factory) OpenModule(ctx context.Context, application schedulersdk.ApplicationRef, host modulehost.ModuleHost) (schedulersdk.Binding, error) {
	if host == nil || host.Migrations() == nil {
		return nil, fmt.Errorf("Scheduler Module migration host is required")
	}
	return moduleassembly.OpenModule(ctx, application, host)
}

func (*Factory) OpenSaaSApplication(ctx context.Context, application schedulersdk.ApplicationRef, host SaaSHost) (schedulersdk.Binding, error) {
	return moduleassembly.OpenSaaS(ctx, application, host)
}

var _ schedulersdk.Factory = (*Factory)(nil)
var _ modulehost.Factory = (*Factory)(nil)
