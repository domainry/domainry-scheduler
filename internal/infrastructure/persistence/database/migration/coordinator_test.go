package migration

import (
	"database/sql"
	"errors"
	"testing"

	ormdialect "github.com/domainry/domainry-orm/dialect"
	ormmigration "github.com/domainry/domainry-orm/migration"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	_ "modernc.org/sqlite"
)

func TestOwnerQualifiedLedgerSharesOnePhysicalTable(t *testing.T) {
	database, err := sql.Open("sqlite", "file:scheduler-owner-ledger?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	database.SetMaxOpenConns(1)
	dialect, err := ormdialect.New(ormdialect.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	renderer := dialect.WithSchema("")
	for _, item := range []struct {
		owner     string
		migration modulehost.SchemaMigration
	}{
		{"scheduler", modulehost.SchemaMigration{Version: 1, Name: "foundation", Statements: []string{"CREATE TABLE scheduler_probe (id TEXT PRIMARY KEY)"}}},
		{"metadata", modulehost.SchemaMigration{Version: 1, Name: "foundation", Statements: []string{"CREATE TABLE metadata_probe (id TEXT PRIMARY KEY)"}}},
	} {
		if err := EnsureSchema(t.Context(), database, renderer, item.owner, []modulehost.SchemaMigration{item.migration}); err != nil {
			t.Fatal(err)
		}
	}
	var ledgers, rows, dirty int
	if err := database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE '%schema_migrations%'`).Scan(&ledgers); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(t.Context(), `SELECT COUNT(*), COALESCE(SUM(CASE WHEN dirty THEN 1 ELSE 0 END), 0) FROM _schema_migrations`).Scan(&rows, &dirty); err != nil {
		t.Fatal(err)
	}
	if ledgers != 1 || rows != 2 || dirty != 0 {
		t.Fatalf("ledgers=%d rows=%d dirty=%d", ledgers, rows, dirty)
	}
	for _, owner := range []string{"scheduler", "metadata"} {
		var count int
		if err := database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _schema_migrations WHERE owner=? AND version=1`, owner).Scan(&count); err != nil || count != 1 {
			t.Fatalf("owner=%s rows=%d err=%v", owner, count, err)
		}
	}
}

func TestOwnerQualifiedLedgerRejectsChecksumDriftAndDirtyRestart(t *testing.T) {
	database, err := sql.Open("sqlite", "file:scheduler-ledger-failures?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	database.SetMaxOpenConns(1)
	dialect, _ := ormdialect.New(ormdialect.SQLite)
	renderer := dialect.WithSchema("")
	base := modulehost.SchemaMigration{Version: 1, Name: "foundation", Statements: []string{"CREATE TABLE stable_probe (id TEXT PRIMARY KEY)"}}
	if err := EnsureSchema(t.Context(), database, renderer, "scheduler", []modulehost.SchemaMigration{base}); err != nil {
		t.Fatal(err)
	}
	drift := base
	drift.Statements = []string{"CREATE TABLE changed_probe (id TEXT PRIMARY KEY)"}
	var migrationErr *ormmigration.Error
	if err := EnsureSchema(t.Context(), database, renderer, "scheduler", []modulehost.SchemaMigration{drift}); !errors.As(err, &migrationErr) || migrationErr.Code != ormmigration.CodeChecksumDrift {
		t.Fatalf("checksum drift error=%v", err)
	}
	broken := modulehost.SchemaMigration{Version: 2, Name: "broken", Statements: []string{"CREATE TABLE"}}
	if err := EnsureSchema(t.Context(), database, renderer, "scheduler", []modulehost.SchemaMigration{broken}); err == nil {
		t.Fatal("broken migration unexpectedly succeeded")
	}
	migrationErr = nil
	if err := EnsureSchema(t.Context(), database, renderer, "scheduler", []modulehost.SchemaMigration{{Version: 2, Name: "broken", Statements: []string{"CREATE TABLE repaired_probe (id TEXT PRIMARY KEY)"}}}); !errors.As(err, &migrationErr) || migrationErr.Code != ormmigration.CodeDirty {
		t.Fatalf("dirty restart error=%v", err)
	}
}
