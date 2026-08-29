package module

import (
	"context"
	"fmt"
	"strings"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulerstore "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence"
)

type Options struct{}

func OptionsFromEnvironment() Options { return Options{} }

type Factory struct{ options Options }

// SaaSHost provides service-owned persistence plus application-specific
// downstream ports. Unlike ModuleHost it does not expose a Runtime migration
// registrar because Scheduler owns the standalone database lifecycle.
type SaaSHost interface {
	modulehost.Host
	Database() modulehost.Database
	Dialect() modulehost.Dialect
	Driver() string
	Schema() string
	WorkerID() string
}

func NewFactory(options Options) *Factory { return &Factory{options: options} }

func (*Factory) Open(context.Context, schedulersdk.ApplicationRef) (schedulersdk.Binding, error) {
	return nil, fmt.Errorf("Scheduler Module host is required")
}

func (*Factory) OpenModule(ctx context.Context, application schedulersdk.ApplicationRef, host modulehost.ModuleHost) (schedulersdk.Binding, error) {
	if host == nil || host.Migrations() == nil {
		return nil, fmt.Errorf("Scheduler Module migration host is required")
	}
	return openHosted(ctx, application, host, schedulersdk.DeploymentModeModule, host.Migrations(), host.Migrations().Driver(), host.Migrations().Schema())
}

func (*Factory) OpenSaaSApplication(ctx context.Context, application schedulersdk.ApplicationRef, host SaaSHost) (schedulersdk.Binding, error) {
	return openHosted(ctx, application, host, schedulersdk.DeploymentModeSaaS, nil, host.Driver(), host.Schema())
}

type persistenceHost interface {
	modulehost.Host
	Database() modulehost.Database
	Dialect() modulehost.Dialect
	WorkerID() string
}

func openHosted(ctx context.Context, application schedulersdk.ApplicationRef, host persistenceHost, mode schedulersdk.DeploymentMode, registrar modulehost.MigrationRegistrar, driver, schema string) (schedulersdk.Binding, error) {
	if err := application.Validate(); err != nil {
		return nil, err
	}
	if host == nil || host.Database() == nil || host.Dialect() == nil || strings.TrimSpace(host.WorkerID()) == "" || host.Definitions() == nil || host.Dispatcher() == nil || host.HTTPConnections() == nil {
		return nil, fmt.Errorf("Scheduler host is incomplete")
	}
	if mode == schedulersdk.DeploymentModeModule {
		if registrar == nil {
			return nil, fmt.Errorf("Scheduler Module migration host is required")
		}
		migrations, err := schedulerstore.SchemaMigrations(driver, schema)
		if err != nil {
			return nil, err
		}
		if err := registrar.ApplyOwnedMigrations(ctx, "scheduler", migrations); err != nil {
			return nil, fmt.Errorf("apply Scheduler Module migrations: %w", err)
		}
	} else if mode == schedulersdk.DeploymentModeSaaS {
		if err := schedulerstore.EnsureSchema(ctx, host.Database(), driver, schema); err != nil {
			return nil, fmt.Errorf("apply Scheduler SaaS migrations: %w", err)
		}
	} else {
		return nil, fmt.Errorf("Scheduler deployment mode %q is unsupported", mode)
	}
	runs, err := schedulerstore.NewStore(host.Database(), host.Dialect(), application.RuntimeID, host.WorkerID())
	if err != nil {
		return nil, err
	}
	ownerCtx, cancel := context.WithCancel(ctx)
	return newBinding(ownerCtx, cancel, application, host, runs, mode), nil
}

var _ schedulersdk.Factory = (*Factory)(nil)
var _ modulehost.Factory = (*Factory)(nil)
