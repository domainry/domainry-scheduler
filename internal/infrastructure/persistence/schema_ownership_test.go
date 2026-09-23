package persistence

import (
	"slices"
	"strings"
	"testing"

	"github.com/domainry/domainry-foundation/schemaownership"
)

func TestSchemaOwnershipMatchesEveryFreshSchedulerTableAndPrimaryKey(t *testing.T) {
	tables := SchemaOwnership()
	if err := schemaownership.ValidateAll(tables); err != nil {
		t.Fatal(err)
	}
	migrations, err := SchemaMigrations("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	created := map[string]string{}
	for _, migration := range migrations {
		for _, statement := range migration.Statements {
			const prefix = `CREATE TABLE "`
			if !strings.HasPrefix(statement, prefix) {
				continue
			}
			name, _, found := strings.Cut(strings.TrimPrefix(statement, prefix), `"`)
			if !found || name == "" {
				t.Fatalf("invalid CREATE TABLE statement: %s", statement)
			}
			created[name] = statement
		}
	}
	if len(created) != len(tables) {
		t.Fatalf("fresh Scheduler tables=%d ownership contracts=%d: created=%v owned=%v", len(created), len(tables), created, OwnedTables())
	}
	for _, table := range tables {
		statement, found := created[table.Name]
		if !found {
			t.Fatalf("Scheduler table %s has ownership but no canonical DDL", table.Name)
		}
		quoted := make([]string, len(table.PrimaryKey))
		for index, column := range table.PrimaryKey {
			quoted[index] = `"` + column + `"`
		}
		if primaryKey := "PRIMARY KEY (" + strings.Join(quoted, ", ") + ")"; !strings.Contains(statement, primaryKey) {
			t.Fatalf("Scheduler table %s ownership primary key %v does not match DDL: %s", table.Name, table.PrimaryKey, statement)
		}
	}
	if !slices.Equal(OwnedTables(), schemaownership.Names(tables)) {
		t.Fatal("Scheduler owned table names drifted from ownership contracts")
	}
}
