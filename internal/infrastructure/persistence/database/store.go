// Package persistence is the Scheduler database composition boundary.
// Concrete repositories live below database/ so hosts never depend on an
// implementation package and repositories never select database engines.
package database

import (
	"context"

	"github.com/domainry/domainry-orm/sqlhost"
	schedulermodulehost "github.com/domainry/domainry-scheduler-sdk/modulehost"
	persistence "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence"
	schedulestore "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence/database/schedule"
)

type Store = schedulestore.Store

func NewStore(database schedulermodulehost.Database, dialect schedulermodulehost.Dialect, runtimeID, workerID string) (*Store, error) {
	return schedulestore.New(database, dialect, runtimeID, workerID)
}

func SchemaMigrations(driver, schema string) ([]schedulermodulehost.SchemaMigration, error) {
	return persistence.SchemaMigrations(driver, schema)
}

func Renderer(driver, schema string) (schedulermodulehost.Dialect, error) {
	return persistence.Renderer(driver, schema)
}

func EnsureSchema(ctx context.Context, database sqlhost.Database, driver, schema string) error {
	return persistence.EnsureSchema(ctx, database, driver, schema)
}
