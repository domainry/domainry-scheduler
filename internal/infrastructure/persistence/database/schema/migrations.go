package schema

import (
	"fmt"

	ormbuilder "github.com/domainry/domainry-orm/builder"
	ormmigration "github.com/domainry/domainry-orm/migration"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
)

const SchemaVersion uint = 1

type Migration = ormmigration.Migration

func Migrations(r modulehost.Dialect) ([]modulehost.SchemaMigration, error) {
	definitions := []struct {
		name    string
		columns []ormbuilder.SchemaColumn
		primary []string
		unique  [][]string
	}{
		{name: "scheduler_schedule_state", columns: []ormbuilder.SchemaColumn{
			required("runtime_id", ormbuilder.TextKeyType(191)), required("definition_key", ormbuilder.TextKeyType(191)),
			required("revision", ormbuilder.TextKeyType(191)), required("enabled", ormbuilder.BooleanType()),
			required("definition_json", ormbuilder.JSONType()), required("next_run_at", ormbuilder.TextKeyType(40)),
			optional("last_run_at", ormbuilder.TextKeyType(40)), optional("last_run_status", ormbuilder.TextKeyType(32)),
			required("snapshot_revision", ormbuilder.BigIntType()), required("updated_at", ormbuilder.TextKeyType(40)),
		}, primary: []string{"runtime_id", "definition_key"}},
		{name: "scheduler_runs", columns: []ormbuilder.SchemaColumn{
			required("runtime_id", ormbuilder.TextKeyType(191)), required("run_id", ormbuilder.TextKeyType(191)),
			required("definition_key", ormbuilder.TextKeyType(191)), required("definition_revision", ormbuilder.TextKeyType(191)),
			required("scheduled_for", ormbuilder.TextKeyType(40)), required("window_key", ormbuilder.TextKeyType(191)),
			required("target_json", ormbuilder.JSONType()), optional("metadata_json", ormbuilder.JSONType()),
			required("status", ormbuilder.TextKeyType(32)), required("attempt", ormbuilder.BigIntType()),
			optional("lease_owner", ormbuilder.TextKeyType(191)), optional("lease_expires_at", ormbuilder.TextKeyType(40)),
			required("fencing_token", ormbuilder.BigIntType()), optional("next_retry_at", ormbuilder.TextKeyType(40)),
			optional("receipt_json", ormbuilder.JSONType()), optional("last_error", ormbuilder.LongTextType()),
			required("created_at", ormbuilder.TextKeyType(40)), required("updated_at", ormbuilder.TextKeyType(40)),
		}, primary: []string{"runtime_id", "run_id"}, unique: [][]string{{"runtime_id", "definition_key", "scheduled_for"}}},
		{name: "scheduler_run_events", columns: []ormbuilder.SchemaColumn{
			required("runtime_id", ormbuilder.TextKeyType(191)), required("event_id", ormbuilder.TextKeyType(191)), required("run_id", ormbuilder.TextKeyType(191)),
			required("event_type", ormbuilder.TextKeyType(64)), optional("message", ormbuilder.LongTextType()), optional("metadata_json", ormbuilder.JSONType()), required("created_at", ormbuilder.TextKeyType(40)),
		}, primary: []string{"runtime_id", "event_id"}},
		{name: "scheduler_dead_letters", columns: []ormbuilder.SchemaColumn{
			required("runtime_id", ormbuilder.TextKeyType(191)), required("run_id", ormbuilder.TextKeyType(191)), required("definition_key", ormbuilder.TextKeyType(191)),
			required("reason", ormbuilder.LongTextType()), required("failed_at", ormbuilder.TextKeyType(40)), optional("resolved_at", ormbuilder.TextKeyType(40)),
		}, primary: []string{"runtime_id", "run_id"}},
	}
	statements := make([]string, 0, len(definitions))
	for _, definition := range definitions {
		builder := ormbuilder.NewCreateTableBuilder(r, definition.name).WithoutSystemColumns().Columns(definition.columns...).PrimaryKey(definition.primary...)
		for _, columns := range definition.unique {
			builder.Unique(columns...)
		}
		statement, _, err := builder.Build()
		if err != nil {
			return nil, fmt.Errorf("build Scheduler table %s: %w", definition.name, err)
		}
		statements = append(statements, statement)
	}
	return []modulehost.SchemaMigration{{Version: SchemaVersion, Name: "scheduler_foundation", Statements: statements}}, nil
}

func required(name string, kind ormbuilder.ColumnType) ormbuilder.SchemaColumn {
	return ormbuilder.DefineColumn(name, kind).NotNull()
}
func optional(name string, kind ormbuilder.ColumnType) ormbuilder.SchemaColumn {
	return ormbuilder.DefineColumn(name, kind)
}
