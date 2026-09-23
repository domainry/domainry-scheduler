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
			if len(migrations) != 1 || len(migrations[0].Statements) != 2 {
				t.Fatalf("unexpected migration inventory: %#v", migrations)
			}
			for _, migration := range migrations {
				for _, statement := range migration.Statements {
					if !strings.Contains(strings.ToLower(statement), "create table") {
						t.Fatalf("expected ORM-rendered table statement, got %q", statement)
					}
				}
			}
			if !strings.Contains(migrations[0].Statements[0], "_scheduler_schedules") {
				t.Fatalf("Scheduler canonical schedule table is missing: %q", migrations[0].Statements[0])
			}
			if strings.Contains(strings.Join(migrations[0].Statements, "\n"), "_worker_scopes") {
				t.Fatal("Scheduler migration still owns the shared Worker Scope table")
			}
			for _, migration := range migrations {
				for _, statement := range migration.Statements {
					if strings.Contains(statement, "_scheduler_dead_letters") || strings.Contains(statement, "_scheduler_run_events") || strings.Contains(statement, "_scheduler_definition_states") || strings.Contains(statement, "_scheduler_plans") || strings.Contains(statement, "_scheduler_command_receipts") || strings.Contains(statement, "_scheduler_definitions") || strings.Contains(statement, "_scheduler_definition_snapshots") || strings.Contains(statement, "_scheduler_definition_publications") {
						t.Fatalf("Scheduler retained a redundant history table: %q", statement)
					}
				}
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
