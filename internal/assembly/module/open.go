// Package module assembles the Scheduler application service with host-owned
// deployment resources. Public Module and SaaS adapters remain thin wrappers
// around this internal composition boundary.
package module

import (
	"context"
	"fmt"
	"strings"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulercapability "github.com/domainry/domainry-scheduler/capability"
	httpexecutor "github.com/domainry/domainry-scheduler/internal/adapter/http"
	application "github.com/domainry/domainry-scheduler/internal/application/scheduler"
	schedulerstore "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence/database"
)

// SaaSHost provides service-owned persistence plus application-specific
// downstream ports. It deliberately has no Runtime migration registrar.
type SaaSHost interface {
	modulehost.Host
	Database() modulehost.Database
	Dialect() modulehost.Dialect
	Driver() string
	Schema() string
	WorkerID() string
}

type persistenceHost interface {
	modulehost.Host
	Database() modulehost.Database
	Dialect() modulehost.Dialect
	WorkerID() string
}

func OpenModule(ctx context.Context, applicationRef schedulersdk.ApplicationRef, host modulehost.ModuleHost) (schedulersdk.Binding, error) {
	if host == nil || host.Migrations() == nil {
		return nil, fmt.Errorf("Scheduler Module migration host is required")
	}
	return open(ctx, applicationRef, host, schedulersdk.DeploymentModeModule, host.Migrations(), host.Migrations().Driver(), host.Migrations().Schema())
}

func OpenSaaS(ctx context.Context, applicationRef schedulersdk.ApplicationRef, host SaaSHost) (schedulersdk.Binding, error) {
	if host == nil {
		return nil, fmt.Errorf("Scheduler SaaS host is required")
	}
	return open(ctx, applicationRef, host, schedulersdk.DeploymentModeSaaS, nil, host.Driver(), host.Schema())
}

func open(ctx context.Context, applicationRef schedulersdk.ApplicationRef, host persistenceHost, mode schedulersdk.DeploymentMode, registrar modulehost.MigrationRegistrar, driver, schema string) (schedulersdk.Binding, error) {
	if err := applicationRef.Validate(); err != nil {
		return nil, err
	}
	if host == nil || host.Database() == nil || host.Dialect() == nil || strings.TrimSpace(host.WorkerID()) == "" || host.Definitions() == nil || host.Dispatcher() == nil || host.HTTPConnections() == nil {
		return nil, fmt.Errorf("Scheduler host is incomplete")
	}
	switch mode {
	case schedulersdk.DeploymentModeModule:
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
	case schedulersdk.DeploymentModeSaaS:
		if err := schedulerstore.EnsureSchema(ctx, host.Database(), driver, schema); err != nil {
			return nil, fmt.Errorf("apply Scheduler SaaS migrations: %w", err)
		}
	default:
		return nil, fmt.Errorf("Scheduler deployment mode %q is unsupported", mode)
	}
	runs, err := schedulerstore.NewStore(host.Database(), host.Dialect(), applicationRef.RuntimeID, host.WorkerID())
	if err != nil {
		return nil, err
	}
	ownerCtx, cancel := context.WithCancel(ctx)
	definitions := schedulerstore.NewDefinitionStore(host.Database(), host.Dialect())
	directHTTP := httpexecutor.New(host.HTTPConnections(), nil)
	capabilityBinding, err := schedulercapability.Open(schedulercapability.Inputs{})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("build Scheduler capability disclosure: %w", err)
	}
	return application.NewService(ownerCtx, cancel, applicationRef, host, directHTTP, runs, definitions, mode, capabilityBinding), nil
}
