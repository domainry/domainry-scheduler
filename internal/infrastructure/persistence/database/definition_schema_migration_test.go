package database

import (
	"database/sql"
	"encoding/json"
	"testing"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	schedulerpersistence "github.com/domainry/domainry-scheduler-sdk/persistence"
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

func TestBoundDefinitionStoreRekeysLegacyRuntimeRowWithoutConstraintMigration(t *testing.T) {
	database, err := sql.Open("sqlite", "file:scheduler-definition-runtime-rekey?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := EnsureSchema(t.Context(), database, "sqlite", ""); err != nil {
		t.Fatal(err)
	}
	legacy := testDefinition("daily", "legacy-a", `{"tenant":"legacy-a"}`)
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `INSERT INTO _scheduler_definitions
		(id, resource_key, object_key, name, payload_json, schema_version, schema_hash, source_kind, source_id, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"scheduler:daily", "daily", "", "Daily", raw, "1", "legacy-hash", "runtime_host", "runtime-a", "now", "now"); err != nil {
		t.Fatal(err)
	}
	dialect, err := Renderer("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	runtimeA, err := NewDefinitionStore(database, dialect, "runtime-a", schedulersdk.DeploymentModeModule)
	if err != nil {
		t.Fatal(err)
	}
	runtimeB, err := NewDefinitionStore(database, dialect, "runtime-b", schedulersdk.DeploymentModeModule)
	if err != nil {
		t.Fatal(err)
	}
	currentA := testDefinition("daily", "current-a", `{"tenant":"a"}`)
	currentB := testDefinition("daily", "current-b", `{"tenant":"b"}`)
	if err := runtimeA.SyncDefinitions(t.Context(), schedulerpersistence.DefinitionSnapshot{Revision: 2, SchemaVersion: "2", SourceKind: "runtime_host", SourceID: "runtime-a", Definitions: []schedulersdk.Definition{currentA}}); err != nil {
		t.Fatal(err)
	}
	if err := runtimeB.SyncDefinitions(t.Context(), schedulerpersistence.DefinitionSnapshot{Revision: 1, SchemaVersion: "1", SourceKind: "runtime_host", SourceID: "runtime-b", Definitions: []schedulersdk.Definition{currentB}}); err != nil {
		t.Fatal(err)
	}
	assertDefinitionSnapshot(t, runtimeA, "runtime-a", map[string]string{"daily": "current-a"})
	assertDefinitionSnapshot(t, runtimeB, "runtime-b", map[string]string{"daily": "current-b"})
	var legacyDisabled sql.NullString
	if err := database.QueryRowContext(t.Context(), `SELECT disabled_at FROM _scheduler_definitions WHERE resource_key = 'daily'`).Scan(&legacyDisabled); err != nil {
		t.Fatal(err)
	}
	if !legacyDisabled.Valid {
		t.Fatal("legacy unscoped definition row remained active")
	}
}
