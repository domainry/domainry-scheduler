package persistence

import (
	"strings"
	"testing"
)

func TestSchemaMigrationsRenderThroughEverySupportedORMEngine(t *testing.T) {
	t.Parallel()
	for _, driver := range []string{"sqlite", "postgres", "mysql"} {
		driver := driver
		t.Run(driver, func(t *testing.T) {
			t.Parallel()
			migrations, err := SchemaMigrations(driver, "scheduler_scope")
			if err != nil {
				t.Fatal(err)
			}
			if len(migrations) != 2 || len(migrations[0].Statements) != 4 || len(migrations[1].Statements) != 1 {
				t.Fatalf("unexpected migration inventory: %#v", migrations)
			}
			for _, migration := range migrations {
				for _, statement := range migration.Statements {
					if !strings.Contains(strings.ToLower(statement), "create table") {
						t.Fatalf("expected ORM-rendered table statement, got %q", statement)
					}
				}
			}
			if !strings.Contains(migrations[1].Statements[0], "_scheduler_definitions") {
				t.Fatalf("Scheduler definition table is not source-owned: %q", migrations[1].Statements[0])
			}
		})
	}
}

func TestNewEngineRejectsUnknownDriver(t *testing.T) {
	t.Parallel()
	if _, err := NewEngine("oracle"); err == nil {
		t.Fatal("expected unsupported driver error")
	}
}
