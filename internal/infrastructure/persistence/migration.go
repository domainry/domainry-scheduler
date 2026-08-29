package persistence

import (
	"context"
	"strings"

	ormmigration "github.com/domainry/domainry-orm/migration"
	"github.com/domainry/domainry-orm/sqlhost"
)

// EnsureSchema applies Scheduler-owned migrations for a standalone SaaS
// database. Embedded Module deployments instead hand SchemaMigrations to the
// host ledger and never create this service-owned ledger.
func EnsureSchema(ctx context.Context, database sqlhost.Database, driver, schema string) error {
	renderer, err := Renderer(driver, schema)
	if err != nil {
		return err
	}
	runner, err := ormmigration.NewRunner(database, renderer, ormmigration.Options{
		InsertConflict: isMigrationConflict,
	})
	if err != nil {
		return err
	}
	migrations, err := SchemaMigrations(driver, schema)
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
