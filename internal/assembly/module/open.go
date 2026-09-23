// Package module assembles the Scheduler application service with host-owned
// deployment resources. Public Module and SaaS adapters remain thin wrappers
// around this internal composition boundary.
package module

import (
	"context"
	"fmt"
	"strings"

	shareddefinition "github.com/domainry/domainry-foundation/definition"
	foundationhttp "github.com/domainry/domainry-foundation/modulehttp"
	sharedoperation "github.com/domainry/domainry-foundation/operation"
	sharedworkerscope "github.com/domainry/domainry-foundation/workerscope"
	metadatasdk "github.com/domainry/domainry-metadata-sdk"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	httpexecutor "github.com/domainry/domainry-scheduler/internal/adapter/http"
	application "github.com/domainry/domainry-scheduler/internal/application/scheduler"
	schedulerstore "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence/database"
	modulehttp "github.com/domainry/domainry-scheduler/internal/transport/http/module"
)

// SaaSHost provides service-owned persistence plus application-specific
// downstream ports. It deliberately has no Runtime migration registrar.
type SaaSHost interface {
	modulehost.Host
	Database() modulehost.Database
	Dialect() modulehost.Dialect
	Migrations() modulehost.MigrationRegistrar
	Driver() string
	Schema() string
	WorkerID() string
}

type persistenceHost interface {
	modulehost.Host
	Database() modulehost.Database
	Dialect() modulehost.Dialect
	Migrations() modulehost.MigrationRegistrar
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
	return open(ctx, applicationRef, host, schedulersdk.DeploymentModeSaaS, host.Migrations(), host.Driver(), host.Schema())
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
		if err := registrar.ApplyOwnedMigrations(ctx, schedulerstore.MigrationOwner, migrations); err != nil {
			return nil, fmt.Errorf("apply Scheduler Module migrations: %w", err)
		}
	case schedulersdk.DeploymentModeSaaS:
		if err := schedulerstore.EnsureSchema(ctx, host.Database(), driver, schema); err != nil {
			return nil, fmt.Errorf("apply Scheduler SaaS migrations: %w", err)
		}
	default:
		return nil, fmt.Errorf("Scheduler deployment mode %q is unsupported", mode)
	}
	definitionKernel, err := shareddefinition.Open(ctx, applicationRef.RuntimeID, host.Database(), host.Dialect(), host.Migrations())
	if err != nil {
		return nil, fmt.Errorf("open Scheduler Definition persistence: %w", err)
	}
	definitionsStore := metadatasdk.AdaptDefinitionStore(definitionKernel)
	operationKernel, err := sharedoperation.Open(ctx, host.Database(), host.Dialect(), host.Migrations())
	if err != nil {
		return nil, fmt.Errorf("open Scheduler Operations persistence: %w", err)
	}
	if _, err := sharedworkerscope.Open(ctx, host.Database(), host.Dialect(), host.Migrations()); err != nil {
		return nil, fmt.Errorf("open Scheduler Worker Scope persistence: %w", err)
	}
	runs, err := schedulerstore.NewStore(host.Database(), host.Dialect(), applicationRef.RuntimeID, host.WorkerID())
	if err != nil {
		return nil, err
	}
	definitions, err := schedulerstore.NewDefinitionStore(host.Database(), host.Dialect(), definitionsStore, applicationRef.RuntimeID, mode)
	if err != nil {
		return nil, err
	}
	plans, err := schedulerstore.NewScheduledPlanStore(host.Database(), host.Dialect(), applicationRef.RuntimeID)
	if err != nil {
		return nil, err
	}
	ownerCtx, cancel := context.WithCancel(ctx)
	directHTTP := httpexecutor.New(host.HTTPConnections(), nil)
	service := application.NewService(ownerCtx, cancel, applicationRef, host, directHTTP, runs, definitions, mode)
	service.SetScheduledPlanRepository(plans)
	if mode == schedulersdk.DeploymentModeModule {
		commandReceipts, err := schedulerstore.NewCommandReceiptStore(operationKernel, applicationRef.RuntimeID)
		if err != nil {
			cancel()
			return nil, err
		}
		adapter, err := modulehttp.NewAdapter(service, commandReceipts)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("build Scheduler Module HTTP adapter: %w", err)
		}
		service.SetHTTPAdapters([]foundationhttp.Adapter{adapter})
	}
	return service, nil
}
