package migration

import (
	"context"
	"strings"

	ormmigration "github.com/domainry/domainry-orm/migration"
	"github.com/domainry/domainry-orm/sqlhost"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
)

// EnsureSchema applies Scheduler-owned migrations for a standalone SaaS
// database. Embedded Module deployments instead hand SchemaMigrations to the
// host ledger and never create this service-owned ledger.
func EnsureSchema(ctx context.Context, database sqlhost.Database, renderer modulehost.Dialect, migrations []modulehost.SchemaMigration) error {
	runner, err := ormmigration.NewRunner(database, renderer, ormmigration.Options{
		InsertConflict: isMigrationConflict,
	})
	if err != nil {
		return err
	}
	return runner.Apply(ctx, migrations)
}

// isMigrationConflict is deliberately kept at the driver boundary until the
// ORM driver profiles expose structured constraint classification.
func isMigrationConflict(err error) bool {
	if err == nil {
		return false
	}
	value := strings.ToLower(err.Error())
	return strings.Contains(value, "unique") || strings.Contains(value, "duplicate") || strings.Contains(value, "constraint")
}
