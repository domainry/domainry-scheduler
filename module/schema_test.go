package module

import (
	"strings"
	"testing"
)

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
