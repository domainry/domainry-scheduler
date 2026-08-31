// Package persistence is the Scheduler database composition boundary.
// Concrete repositories live below database/ so hosts never depend on an
// implementation package and repositories never select database engines.
package database

import (
	"context"

	"github.com/domainry/domainry-orm/sqlhost"
	schedulermodulehost "github.com/domainry/domainry-scheduler-sdk/modulehost"
	"github.com/domainry/domainry-scheduler/internal/infrastructure/persistence"
	"github.com/domainry/domainry-scheduler/internal/infrastructure/persistence/database/migration"
	schedulerstore "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence/database/scheduler"
)

type Store = schedulerstore.Store
type DefinitionStore = schedulerstore.DefinitionStore

func NewStore(database schedulermodulehost.Database, dialect schedulermodulehost.Dialect, runtimeID, workerID string) (*Store, error) {
	return schedulerstore.New(database, dialect, runtimeID, workerID)
}

func NewDefinitionStore(database schedulermodulehost.Database, dialect schedulermodulehost.Dialect) DefinitionStore {
	return schedulerstore.NewDefinitionStore(database, dialect)
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
	return migration.EnsureSchema(ctx, database, renderer, migrations)
}
