// Package persistence is the Scheduler database composition boundary.
// Concrete repositories live below database/ so hosts never depend on an
// implementation package and repositories never select database engines.
package database

import (
	"context"

	sharedoperation "github.com/domainry/domainry-foundation/operation"
	sharedworkerscope "github.com/domainry/domainry-foundation/workerscope"
	metadatasdk "github.com/domainry/domainry-metadata-sdk"
	"github.com/domainry/domainry-orm/sqlhost"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	schedulermodulehost "github.com/domainry/domainry-scheduler-sdk/modulehost"
	"github.com/domainry/domainry-scheduler/internal/infrastructure/persistence"
	"github.com/domainry/domainry-scheduler/internal/infrastructure/persistence/database/migration"
	schedulerstore "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence/database/scheduler"
)

type Store = schedulerstore.Store
type DefinitionStore = schedulerstore.DefinitionStore
type CommandReceiptStore = schedulerstore.CommandReceiptStore
type ScheduledPlanStore = schedulerstore.ScheduledPlanStore

func NewStore(database schedulermodulehost.Database, dialect schedulermodulehost.Dialect, runtimeID, workerID string) (*Store, error) {
	return schedulerstore.New(database, dialect, runtimeID, workerID)
}

func NewDefinitionStore(database schedulermodulehost.Database, dialect schedulermodulehost.Dialect, shared metadatasdk.DefinitionStore, runtimeID string, mode schedulersdk.DeploymentMode) (DefinitionStore, error) {
	return schedulerstore.NewDefinitionStore(database, dialect, shared, runtimeID, mode)
}

func NewCommandReceiptStore(operations sharedoperation.Store, runtimeID string) (*CommandReceiptStore, error) {
	return schedulerstore.NewCommandReceiptStore(operations, runtimeID)
}

func NewScheduledPlanStore(database schedulermodulehost.Database, dialect schedulermodulehost.Dialect, runtimeID string) (*ScheduledPlanStore, error) {
	return schedulerstore.NewScheduledPlanStore(database, dialect, runtimeID)
}

func SchemaMigrations(driver, schema string) ([]schedulermodulehost.SchemaMigration, error) {
	return persistence.SchemaMigrations(driver, schema)
}

func Renderer(driver, schema string) (schedulermodulehost.Dialect, error) {
	return persistence.Renderer(driver, schema)
}

func EnsureSchema(ctx context.Context, database sqlhost.Database, driver, schema string) error {
	renderer, err := persistence.Renderer(driver, schema)
	if err != nil {
		return err
	}
	migrations, err := persistence.SchemaMigrations(driver, schema)
	if err != nil {
		return err
	}
	if err := migration.EnsureSchema(ctx, database, renderer, "scheduler", migrations); err != nil {
		return err
	}
	_, err = sharedworkerscope.Open(ctx, database, renderer, schedulerSaaSWorkerScopeMigrations{database: database, renderer: renderer})
	return err
}

type schedulerSaaSWorkerScopeMigrations struct {
	database sqlhost.Database
	renderer schedulermodulehost.Dialect
}

func (m schedulerSaaSWorkerScopeMigrations) ApplyOwnedMigrations(ctx context.Context, owner string, migrations []sharedworkerscope.SchemaMigration) error {
	return migration.EnsureSchema(ctx, m.database, m.renderer, owner, migrations)
}
