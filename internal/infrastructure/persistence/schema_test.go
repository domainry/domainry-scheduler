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
			if len(migrations) != 8 || len(migrations[0].Statements) != 4 || len(migrations[1].Statements) != 1 || len(migrations[2].Statements) != 1 || len(migrations[3].Statements) != 1 || len(migrations[4].Statements) != 1 || len(migrations[5].Statements) != 1 || len(migrations[6].Statements) != 1 || len(migrations[7].Statements) != 1 {
				t.Fatalf("unexpected migration inventory: %#v", migrations)
			}
			for _, migration := range migrations {
				for _, statement := range migration.Statements {
					if migration.Version < 7 && !strings.Contains(strings.ToLower(statement), "create table") {
						t.Fatalf("expected ORM-rendered table statement, got %q", statement)
					}
				}
			}
			if !strings.Contains(migrations[1].Statements[0], "_scheduler_definitions") {
				t.Fatalf("Scheduler definition table is not source-owned: %q", migrations[1].Statements[0])
			}
			if !strings.Contains(migrations[2].Statements[0], "_scheduler_command_receipts") {
				t.Fatalf("Scheduler command receipt table is not source-owned: %q", migrations[2].Statements[0])
			}
			if !strings.Contains(migrations[3].Statements[0], "_scheduler_definition_snapshots") {
				t.Fatalf("Scheduler definition snapshot state is not source-owned: %q", migrations[3].Statements[0])
			}
			if !strings.Contains(migrations[4].Statements[0], "_scheduler_definition_publications") {
				t.Fatalf("Scheduler definition publication state is not source-owned: %q", migrations[4].Statements[0])
			}
			if !strings.Contains(migrations[5].Statements[0], "_scheduler_plans") {
				t.Fatalf("Scheduler plan table is not source-owned: %q", migrations[5].Statements[0])
			}
			if driver == "mysql" && strings.Contains(strings.ToUpper(migrations[5].Statements[0]), "UNIQUE") {
				t.Fatalf("Scheduler plan identity is already enforced by its deterministic plan ID and primary key; an additional utf8mb4 owner/client unique index exceeds MySQL's 3072-byte key limit: %q", migrations[5].Statements[0])
			}
			if !strings.Contains(migrations[6].Statements[0], "_scheduler_definition_states") || !strings.Contains(strings.ToLower(migrations[6].Statements[0]), "add column") || !strings.Contains(migrations[6].Statements[0], "source_kind") {
				t.Fatalf("Scheduler definition state source migration is missing: %q", migrations[6].Statements[0])
			}
			if !strings.Contains(migrations[7].Statements[0], "_scheduler_capacity_guards") {
				t.Fatalf("Scheduler capacity guard table is missing: %q", migrations[7].Statements[0])
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
