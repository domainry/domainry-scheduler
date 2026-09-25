package schema

import (
	"fmt"

	"github.com/domainry/domainry-foundation/schemaownership"
	ormschema "github.com/domainry/domainry-orm/schema"

	ormmigration "github.com/domainry/domainry-orm/migration"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
)

const (
	SchemaVersion     uint = 1
	MigrationOwner         = "scheduler"
	ScheduleTableName      = "_scheduler_schedules"
	RunTableName           = "_scheduler_runs"
)

type Migration = ormmigration.Migration

func Migrations(r modulehost.Dialect) ([]modulehost.SchemaMigration, error) {
	definitions := []struct {
		name    string
		columns []ormschema.ColumnDefinition
		primary []string
		unique  [][]string
	}{
		{name: ScheduleTableName, columns: []ormschema.ColumnDefinition{
			required("runtime_id", ormschema.TextKey(191)), required("schedule_id", ormschema.TextKey(191)),
			required("kind", ormschema.TextKey(32)), required("source_kind", ormschema.TextKey(32)),
			required("source_id", ormschema.TextKey(191)), optional("definition_revision", ormschema.TextKey(191)),
			required("enabled", ormschema.Boolean()), optional("definition_json", ormschema.JSON()),
			optional("next_run_at", ormschema.BigInt()), optional("last_run_at", ormschema.BigInt()),
			optional("last_run_status", ormschema.TextKey(32)), required("snapshot_revision", ormschema.BigInt()),
			optional("publication_generation", ormschema.BigInt()), optional("publication_session_sha256", ormschema.TextKey(64)),
			optional("publication_content_sha256", ormschema.TextKey(64)),
			optional("client_id", ormschema.TextKey(191)), optional("request_sha256", ormschema.TextKey(64)),
			optional("workspace_id", ormschema.TextKey(191)), optional("user_id", ormschema.TextKey(191)),
			optional("product_key", ormschema.TextKey(191)), optional("status", ormschema.TextKey(32)),
			optional("plan_revision", ormschema.BigInt()), optional("plan_json", ormschema.JSON()),
			required("created_at", ormschema.BigInt()), required("updated_at", ormschema.BigInt()),
		}, primary: []string{"runtime_id", "schedule_id"}},
		{name: RunTableName, columns: []ormschema.ColumnDefinition{
			required("runtime_id", ormschema.TextKey(191)), required("run_id", ormschema.TextKey(191)),
			required("definition_key", ormschema.TextKey(191)), required("definition_revision", ormschema.TextKey(191)),
			required("scheduled_for", ormschema.BigInt()), required("window_key", ormschema.TextKey(191)),
			required("target_json", ormschema.JSON()), optional("metadata_json", ormschema.JSON()),
			required("status", ormschema.TextKey(32)), required("attempt", ormschema.BigInt()),
			optional("lease_owner", ormschema.TextKey(191)), optional("lease_expires_at", ormschema.BigInt()),
			required("fencing_token", ormschema.BigInt()), optional("next_retry_at", ormschema.BigInt()),
			optional("receipt_json", ormschema.JSON()), optional("last_error", ormschema.LongText()),
			optional("failed_at", ormschema.BigInt()), optional("dead_letter_resolved_at", ormschema.BigInt()),
			optional("dead_letter_resolved_by", ormschema.TextKey(191)), optional("dead_letter_resolution_operation_id", ormschema.TextKey(191)),
			optional("dead_letter_resolution_reason", ormschema.LongText()),
			required("created_at", ormschema.BigInt()), required("updated_at", ormschema.BigInt()),
		}, primary: []string{"runtime_id", "run_id"}, unique: [][]string{{"runtime_id", "definition_key", "scheduled_for"}}},
	}
	statements := make([]string, 0, len(definitions))
	for _, definition := range definitions {
		builder := ormschema.NewTable(r, definition.name).Columns(definition.columns...).PrimaryKey(definition.primary...)
		for _, columns := range definition.unique {
			builder.Unique(columns...)
		}
		statement, _, err := builder.Build()
		if err != nil {
			return nil, fmt.Errorf("build Scheduler table %s: %w", definition.name, err)
		}
		statements = append(statements, statement)
	}
	return []modulehost.SchemaMigration{{Version: 1, Name: "scheduler_foundation", Statements: statements}}, nil
}

func SchemaOwnership() []schemaownership.Table {
	return []schemaownership.Table{
		{
			Name: ScheduleTableName, Owner: MigrationOwner, WorkspaceScope: schemaownership.ScopeExplicitMixed,
			RetentionClass: schemaownership.RetentionProduct, PrimaryKey: []string{"runtime_id", "schedule_id"},
			BoundedQueryPath: "runtime plus schedule identity; workspace/user/product plan cursor and due-schedule scans enforce limits",
			DeletionPolicy:   "scheduled plans are soft-deleted in status; definition and plan state remain for product retention",
		},
		{
			Name: RunTableName, Owner: MigrationOwner, WorkspaceScope: schemaownership.ScopeExplicitMixed,
			RetentionClass: schemaownership.RetentionProduct, PrimaryKey: []string{"runtime_id", "run_id"},
			BoundedQueryPath: "runtime plus run identity; due, retry, history and dead-letter scans enforce limits",
			DeletionPolicy:   "terminal and resolved dead-letter runs remain as product execution history; no physical purge path is registered",
		},
	}
}

func OwnedTables() []string { return schemaownership.Names(SchemaOwnership()) }

func required(name string, kind ormschema.ColumnType) ormschema.ColumnDefinition {
	return ormschema.Column(name, kind).NotNull()
}
func optional(name string, kind ormschema.ColumnType) ormschema.ColumnDefinition {
	return ormschema.Column(name, kind)
}
