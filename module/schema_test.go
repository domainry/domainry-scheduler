package module

import (
	"slices"
	"strings"
	"testing"

	"github.com/domainry/domainry-foundation/schemaownership"
)

func TestModulePublishesSchedulerOwnedSchema(t *testing.T) {
	tables := SchemaOwnership()
	if err := schemaownership.ValidateAll(tables); err != nil {
		t.Fatal(err)
	}
	if len(tables) != 2 || !slices.Equal(OwnedTables(), schemaownership.Names(tables)) {
		t.Fatalf("Scheduler schema ownership=%d tables=%v", len(tables), OwnedTables())
	}
}

func TestModulePublishesCanonicalSchedulerMigrations(t *testing.T) {
	migrations, err := SchemaMigrations("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	statements := ""
	for _, migration := range migrations {
		statements += strings.Join(migration.Statements, "\n")
	}
	for _, table := range OwnedTables() {
		if !strings.Contains(statements, `"`+table+`"`) {
			t.Fatalf("canonical Scheduler migrations omit %s", table)
		}
	}
}
