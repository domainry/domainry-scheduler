package persistence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	ormbuilder "github.com/domainry/domainry-orm/builder"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulerrepository "github.com/domainry/domainry-scheduler-sdk/repository"
)

type DefinitionStore struct {
	database modulehost.Database
	dialect  modulehost.Dialect
}

func NewDefinitionStore(database modulehost.Database, dialect modulehost.Dialect) DefinitionStore {
	return DefinitionStore{database: database, dialect: dialect}
}

func (s DefinitionStore) SyncDefinitions(ctx context.Context, snapshot schedulerrepository.DefinitionSnapshot) error {
	if s.database == nil || s.dialect == nil {
		return fmt.Errorf("Scheduler definition store is unavailable")
	}
	if snapshot.Revision < 0 || strings.TrimSpace(snapshot.SchemaVersion) == "" || strings.TrimSpace(snapshot.SourceKind) == "" || strings.TrimSpace(snapshot.SourceID) == "" {
		return fmt.Errorf("Scheduler definition snapshot identity is required")
	}
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	disable, args, err := ormbuilder.NewUpdateBuilder(s.dialect, "scheduler_definitions").Set("disabled_at", now).Where(ormbuilder.And(
		ormbuilder.Equal("source_kind", snapshot.SourceKind), ormbuilder.Equal("source_id", snapshot.SourceID), ormbuilder.IsNull("disabled_at"),
	)).Build()
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, disable, args...); err != nil {
		return err
	}
	for _, definition := range snapshot.Definitions {
		definition = definition.Normalize()
		if err := definition.Validate(); err != nil {
			return err
		}
		raw, err := json.Marshal(definition)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(raw)
		key := strings.TrimSpace(definition.Key)
		update, updateArgs, err := ormbuilder.NewUpdateBuilder(s.dialect, "scheduler_definitions").
			Set("name", definition.Name).Set("payload_json", raw).Set("schema_version", snapshot.SchemaVersion).
			Set("schema_hash", hex.EncodeToString(sum[:])).Set("source_kind", snapshot.SourceKind).
			Set("source_id", snapshot.SourceID).Set("disabled_at", nil).Set("updated_at", now).
			Where(ormbuilder.Equal("resource_key", key)).Build()
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, update, updateArgs...)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected > 0 {
			continue
		}
		statement, values, err := ormbuilder.NewInsertBuilder(s.dialect, "scheduler_definitions").Columns(
			"id", "resource_key", "object_key", "name", "payload_json", "schema_version", "schema_hash", "source_kind", "source_id", "disabled_at", "created_at", "updated_at",
		).Values("scheduler:"+key, key, "", definition.Name, raw, snapshot.SchemaVersion, hex.EncodeToString(sum[:]), snapshot.SourceKind, snapshot.SourceID, nil, now, now).Build()
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, statement, values...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s DefinitionStore) DefinitionSnapshot(ctx context.Context) (schedulerrepository.DefinitionSnapshot, error) {
	if s.database == nil || s.dialect == nil {
		return schedulerrepository.DefinitionSnapshot{}, fmt.Errorf("Scheduler definition store is unavailable")
	}
	statement, args, err := ormbuilder.NewSelectBuilder(s.dialect, "scheduler_definitions").Columns(
		"payload_json", "schema_version", "source_kind", "source_id",
	).Where(ormbuilder.IsNull("disabled_at")).OrderBy(ormbuilder.Ascending("resource_key")).Build()
	if err != nil {
		return schedulerrepository.DefinitionSnapshot{}, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return schedulerrepository.DefinitionSnapshot{}, err
	}
	defer rows.Close()
	result := schedulerrepository.DefinitionSnapshot{Definitions: []schedulersdk.Definition{}}
	for rows.Next() {
		var raw []byte
		var version, sourceKind, sourceID string
		if err := rows.Scan(&raw, &version, &sourceKind, &sourceID); err != nil {
			return schedulerrepository.DefinitionSnapshot{}, err
		}
		var definition schedulersdk.Definition
		if err := json.Unmarshal(raw, &definition); err != nil {
			return schedulerrepository.DefinitionSnapshot{}, err
		}
		result.Definitions = append(result.Definitions, definition)
		if result.SchemaVersion == "" {
			result.SchemaVersion, result.SourceKind, result.SourceID = version, sourceKind, sourceID
		}
	}
	if err := rows.Err(); err != nil {
		return schedulerrepository.DefinitionSnapshot{}, err
	}
	raw, _ := json.Marshal(result.Definitions)
	sum := sha256.Sum256(raw)
	result.SchemaHash = hex.EncodeToString(sum[:])
	return result, nil
}

var _ schedulerrepository.DefinitionRepository = DefinitionStore{}
