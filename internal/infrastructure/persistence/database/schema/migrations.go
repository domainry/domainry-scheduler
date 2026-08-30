package schema

import (
	"fmt"
	ormschema "github.com/domainry/domainry-orm/schema"

	ormmigration "github.com/domainry/domainry-orm/migration"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
)

const SchemaVersion uint = 2

type Migration = ormmigration.Migration

func Migrations(r modulehost.Dialect) ([]modulehost.SchemaMigration, error) {
	definitions := []struct {
		name    string
		columns []ormschema.ColumnDefinition
		primary []string
		unique  [][]string
	}{
		{name: "scheduler_schedule_state", columns: []ormschema.ColumnDefinition{
			required("runtime_id", ormschema.TextKey(191)), required("definition_key", ormschema.TextKey(191)),
			required("revision", ormschema.TextKey(191)), required("enabled", ormschema.Boolean()),
			required("definition_json", ormschema.JSON()), required("next_run_at", ormschema.TextKey(40)),
			optional("last_run_at", ormschema.TextKey(40)), optional("last_run_status", ormschema.TextKey(32)),
			required("snapshot_revision", ormschema.BigInt()), required("updated_at", ormschema.TextKey(40)),
		}, primary: []string{"runtime_id", "definition_key"}},
		{name: "scheduler_runs", columns: []ormschema.ColumnDefinition{
			required("runtime_id", ormschema.TextKey(191)), required("run_id", ormschema.TextKey(191)),
			required("definition_key", ormschema.TextKey(191)), required("definition_revision", ormschema.TextKey(191)),
			required("scheduled_for", ormschema.TextKey(40)), required("window_key", ormschema.TextKey(191)),
			required("target_json", ormschema.JSON()), optional("metadata_json", ormschema.JSON()),
			required("status", ormschema.TextKey(32)), required("attempt", ormschema.BigInt()),
			optional("lease_owner", ormschema.TextKey(191)), optional("lease_expires_at", ormschema.TextKey(40)),
			required("fencing_token", ormschema.BigInt()), optional("next_retry_at", ormschema.TextKey(40)),
			optional("receipt_json", ormschema.JSON()), optional("last_error", ormschema.LongText()),
			required("created_at", ormschema.TextKey(40)), required("updated_at", ormschema.TextKey(40)),
		}, primary: []string{"runtime_id", "run_id"}, unique: [][]string{{"runtime_id", "definition_key", "scheduled_for"}}},
		{name: "scheduler_run_events", columns: []ormschema.ColumnDefinition{
			required("runtime_id", ormschema.TextKey(191)), required("event_id", ormschema.TextKey(191)), required("run_id", ormschema.TextKey(191)),
			required("event_type", ormschema.TextKey(64)), optional("message", ormschema.LongText()), optional("metadata_json", ormschema.JSON()), required("created_at", ormschema.TextKey(40)),
		}, primary: []string{"runtime_id", "event_id"}},
		{name: "scheduler_dead_letters", columns: []ormschema.ColumnDefinition{
			required("runtime_id", ormschema.TextKey(191)), required("run_id", ormschema.TextKey(191)), required("definition_key", ormschema.TextKey(191)),
			required("reason", ormschema.LongText()), required("failed_at", ormschema.TextKey(40)), optional("resolved_at", ormschema.TextKey(40)),
		}, primary: []string{"runtime_id", "run_id"}},
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
	definitionStatement, _, err := definitionTable(r).Build()
	if err != nil {
		return nil, fmt.Errorf("build Scheduler definition table: %w", err)
	}
	return []modulehost.SchemaMigration{
		{Version: 1, Name: "scheduler_foundation", Statements: statements},
		{Version: 2, Name: "scheduler_definition_ownership", Statements: []string{definitionStatement}},
	}, nil
}

// definitionTable is the canonical Scheduler-authored definition projection.
// The host supplies the database, dialect, lock, transaction boundary and
// migration ledger, but must not declare this module-owned table itself.
func definitionTable(r modulehost.Dialect) *ormschema.TableBuilder {
	return ormschema.NewTable(r, "scheduler_definitions").IfNotExists().Columns(
		required("id", ormschema.TextKey(255)),
		required("resource_key", ormschema.TextKey(255)),
		required("object_key", ormschema.TextKey(255)),
		required("name", ormschema.Text()),
		required("payload_json", ormschema.LongText()),
		required("schema_version", ormschema.TextKey(255)),
		required("schema_hash", ormschema.TextKey(255)),
		required("source_kind", ormschema.TextKey(255)),
		required("source_id", ormschema.TextKey(255)),
		optional("disabled_at", ormschema.TextKey(255)),
		required("created_at", ormschema.TextKey(255)),
		required("updated_at", ormschema.TextKey(255)),
	).PrimaryKey("id").Unique("resource_key")
}

func required(name string, kind ormschema.ColumnType) ormschema.ColumnDefinition {
	return ormschema.Column(name, kind).NotNull()
}
func optional(name string, kind ormschema.ColumnType) ormschema.ColumnDefinition {
	return ormschema.Column(name, kind)
}
