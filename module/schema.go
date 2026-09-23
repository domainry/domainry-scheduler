package module

import (
	"github.com/domainry/domainry-foundation/schemaownership"
	schedulerstore "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence/database"
)

// SchemaOwnership returns only Scheduler-owned physical tables. Shared
// Foundation tables are registered by their source-owning packages.
func SchemaOwnership() []schemaownership.Table { return schedulerstore.SchemaOwnership() }

func OwnedTables() []string { return schemaownership.Names(SchemaOwnership()) }
