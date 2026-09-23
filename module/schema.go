package module

import (
	"github.com/domainry/domainry-foundation/schemaownership"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulerstore "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence/database"
)

// SchemaOwnership returns only Scheduler-owned physical tables. Shared
// Foundation tables are registered by their source-owning packages.
func SchemaOwnership() []schemaownership.Table { return schedulerstore.SchemaOwnership() }

func OwnedTables() []string { return schemaownership.Names(SchemaOwnership()) }

// SchemaMigrations exposes Scheduler's canonical source-owned DDL for
// cross-module composition verification without exposing its Store.
func SchemaMigrations(driver, schema string) ([]modulehost.SchemaMigration, error) {
	return schedulerstore.SchemaMigrations(driver, schema)
}
