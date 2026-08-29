package module

import (
	"context"
	"database/sql"
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
	Database() *sql.DB
	Driver() string
	Schema() string
	WorkerID() string
}

func NewFactory(options Options) *Factory { return &Factory{options: options} }

func (*Factory) Open(context.Context, schedulersdk.ApplicationRef) (schedulersdk.Binding, error) {
	return nil, fmt.Errorf("Scheduler Module host is required")
}

func (*Factory) OpenModule(ctx context.Context, application schedulersdk.ApplicationRef, host modulehost.ModuleHost) (schedulersdk.Binding, error) {
	return openHosted(ctx, application, host, schedulersdk.DeploymentModeModule, host.Migrations())
}

func (*Factory) OpenSaaSApplication(ctx context.Context, application schedulersdk.ApplicationRef, host SaaSHost) (schedulersdk.Binding, error) {
	return openHosted(ctx, application, host, schedulersdk.DeploymentModeSaaS, nil)
}

func openHosted(ctx context.Context, application schedulersdk.ApplicationRef, host SaaSHost, mode schedulersdk.DeploymentMode, registrar modulehost.MigrationRegistrar) (schedulersdk.Binding, error) {
	if err := application.Validate(); err != nil {
		return nil, err
	}
	if host == nil || host.Database() == nil || strings.TrimSpace(host.Driver()) == "" || strings.TrimSpace(host.WorkerID()) == "" || host.Definitions() == nil || host.Dispatcher() == nil || host.HTTPConnections() == nil {
		return nil, fmt.Errorf("Scheduler host is incomplete")
	}
	if mode == schedulersdk.DeploymentModeModule {
		if registrar == nil {
			return nil, fmt.Errorf("Scheduler Module migration host is required")
		}
		migrations, err := schedulerstore.SchemaMigrations(host.Driver(), host.Schema())
		if err != nil {
			return nil, err
		}
		if err := registrar.ApplyOwnedMigrations(ctx, "scheduler", migrations); err != nil {
			return nil, fmt.Errorf("apply Scheduler Module migrations: %w", err)
		}
	} else if mode == schedulersdk.DeploymentModeSaaS {
		if err := schedulerstore.EnsureSchema(ctx, host.Database(), host.Driver(), host.Schema()); err != nil {
			return nil, fmt.Errorf("apply Scheduler SaaS migrations: %w", err)
		}
	} else {
		return nil, fmt.Errorf("Scheduler deployment mode %q is unsupported", mode)
	}
	runs, err := schedulerstore.NewStore(host.Database(), host.Driver(), host.Schema(), application.RuntimeID, host.WorkerID())
	if err != nil {
		return nil, err
	}
	ownerCtx, cancel := context.WithCancel(ctx)
	return newBinding(ownerCtx, cancel, application, host, runs, mode), nil
}

var _ schedulersdk.Factory = (*Factory)(nil)
var _ modulehost.Factory = (*Factory)(nil)
