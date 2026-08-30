package persistence

import (
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

func TestSchedulerDefinitionOwnershipMigrationAdoptsRuntimeCreatedTable(t *testing.T) {
	database, err := sql.Open("sqlite", "file:scheduler-definition-adoption?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.ExecContext(t.Context(), `CREATE TABLE _scheduler_definitions (
		id TEXT PRIMARY KEY, resource_key TEXT NOT NULL UNIQUE, object_key TEXT NOT NULL,
		name TEXT NOT NULL, payload_json TEXT NOT NULL, schema_version TEXT NOT NULL,
		schema_hash TEXT NOT NULL, source_kind TEXT NOT NULL, source_id TEXT NOT NULL,
		disabled_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `INSERT INTO _scheduler_definitions
		(id, resource_key, object_key, name, payload_json, schema_version, schema_hash, source_kind, source_id, created_at, updated_at)
		VALUES ('scheduler:daily', 'daily', '', 'Daily', '{}', '1', 'hash', 'manifest', 'template', 'now', 'now')`); err != nil {
		t.Fatal(err)
	}
	if err := EnsureSchema(t.Context(), database, "sqlite", ""); err != nil {
		t.Fatal(err)
	}
	var rows int
	if err := database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _scheduler_definitions WHERE resource_key = 'daily'`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("adopted Scheduler definitions=%d", rows)
	}
}
