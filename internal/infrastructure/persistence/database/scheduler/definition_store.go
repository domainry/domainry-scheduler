package scheduler

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/domainry/domainry-orm/query"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulerpersistence "github.com/domainry/domainry-scheduler-sdk/persistence"
)

type DefinitionStore struct {
	database modulehost.Database
	dialect  modulehost.Dialect
	sourceID string
	mode     schedulersdk.DeploymentMode
}

const runtimeDefinitionSourceKind = "runtime_host"

// The wire contract currently encodes generation as a JSON number. Keep it in
// both signed BIGINT and IEEE-754 exact-integer range until the protocol moves
// to a string representation.
const maxDefinitionPublisherGeneration uint64 = 1<<53 - 1

var errDefinitionPublicationCAS = errors.New("Scheduler definition publication compare-and-swap lost")

type encodedDefinition struct {
	definition schedulersdk.Definition
	raw        []byte
	hash       string
	storageKey string
}

func NewDefinitionStore(database modulehost.Database, dialect modulehost.Dialect, runtimeID string, mode schedulersdk.DeploymentMode) (DefinitionStore, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if database == nil || dialect == nil {
		return DefinitionStore{}, fmt.Errorf("Scheduler definition store database and dialect are required")
	}
	if runtimeID == "" {
		return DefinitionStore{}, fmt.Errorf("Scheduler definition store Runtime identity is required")
	}
	if mode != schedulersdk.DeploymentModeModule && mode != schedulersdk.DeploymentModeSaaS {
		return DefinitionStore{}, fmt.Errorf("Scheduler definition store deployment mode is required")
	}
	return DefinitionStore{database: database, dialect: dialect, sourceID: runtimeID, mode: mode}, nil
}

func (s DefinitionStore) SyncDefinitions(ctx context.Context, snapshot schedulerpersistence.DefinitionSnapshot) error {
	if s.database == nil || s.dialect == nil {
		return fmt.Errorf("Scheduler definition store is unavailable")
	}
	if snapshot.Revision <= 0 || strings.TrimSpace(snapshot.SchemaVersion) == "" || strings.TrimSpace(snapshot.SourceKind) == "" || strings.TrimSpace(snapshot.SourceID) == "" {
		return fmt.Errorf("Scheduler definition snapshot identity is required")
	}
	if strings.TrimSpace(snapshot.SourceKind) != runtimeDefinitionSourceKind || strings.TrimSpace(snapshot.SourceID) != s.sourceID {
		return fmt.Errorf("Scheduler definition snapshot does not match the bound Runtime application")
	}
	encoded, snapshotHash, err := s.encodeDefinitions(snapshot.Definitions)
	if err != nil {
		return err
	}
	if snapshot.ContentSHA256 != "" && snapshot.ContentSHA256 != snapshotHash {
		return fmt.Errorf("Scheduler definition snapshot content hash does not match its definitions")
	}
	switch s.mode {
	case schedulersdk.DeploymentModeModule:
		if snapshot.PublisherFence != nil {
			return fmt.Errorf("Scheduler Module definition snapshot must not carry a publisher fence")
		}
	case schedulersdk.DeploymentModeSaaS:
		if snapshot.PublisherFence == nil {
			return schedulersdk.ErrDefinitionPublicationRequired
		}
	default:
		return fmt.Errorf("Scheduler definition store deployment mode is invalid")
	}
	var lastErr error
	for attempt := 0; attempt < 64; attempt++ {
		err := s.syncDefinitionsOnce(ctx, snapshot, encoded, snapshotHash)
		if !errors.Is(err, errDefinitionPublicationCAS) {
			return err
		}
		lastErr = err
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := waitDefinitionPublicationRetry(ctx, attempt); err != nil {
			return err
		}
	}
	return lastErr
}

func (s DefinitionStore) syncDefinitionsOnce(ctx context.Context, snapshot schedulerpersistence.DefinitionSnapshot, encoded []encodedDefinition, snapshotHash string) error {
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if snapshot.PublisherFence != nil {
		disposition, err := s.acceptFencedSnapshot(ctx, tx, snapshot, snapshotHash, now)
		if err != nil {
			return err
		}
		if disposition == schedulersdk.DefinitionSnapshotReplay {
			return tx.Commit()
		}
	}
	if err := s.writeSnapshotState(ctx, tx, snapshot, snapshotHash, now); err != nil {
		return err
	}
	disable, args, err := query.NewUpdateBuilder(s.dialect, "_scheduler_definitions").Set("disabled_at", now).Where(query.And(
		query.Equal("source_kind", runtimeDefinitionSourceKind), query.Equal("source_id", s.sourceID), query.IsNull("disabled_at"),
	)).Build()
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, disable, args...); err != nil {
		return err
	}
	for _, value := range encoded {
		definition, raw, key, storageKey := value.definition, value.raw, strings.TrimSpace(value.definition.Key), value.storageKey
		update, updateArgs, err := query.NewUpdateBuilder(s.dialect, "_scheduler_definitions").
			Set("object_key", key).Set("name", definition.Name).Set("payload_json", raw).Set("schema_version", snapshot.SchemaVersion).
			Set("schema_hash", value.hash).Set("disabled_at", nil).Set("updated_at", now).
			Where(query.And(
				query.Equal("source_kind", runtimeDefinitionSourceKind),
				query.Equal("source_id", s.sourceID),
				query.Equal("resource_key", storageKey),
			)).Build()
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
		statement, values, err := query.NewInsertBuilder(s.dialect, "_scheduler_definitions").Columns(
			"id", "resource_key", "object_key", "name", "payload_json", "schema_version", "schema_hash", "source_kind", "source_id", "disabled_at", "created_at", "updated_at",
		).Values("scheduler:"+storageKey, storageKey, key, definition.Name, raw, snapshot.SchemaVersion, value.hash, runtimeDefinitionSourceKind, s.sourceID, nil, now, now).Build()
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, statement, values...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s DefinitionStore) BeginDefinitionPublisherSession(ctx context.Context) (schedulersdk.DefinitionPublisherSession, error) {
	if s.mode != schedulersdk.DeploymentModeSaaS {
		return schedulersdk.DefinitionPublisherSession{}, schedulersdk.ErrDefinitionPublicationCapabilityRequired
	}
	var lastErr error
	for attempt := 0; attempt < 64; attempt++ {
		if err := ctx.Err(); err != nil {
			return schedulersdk.DefinitionPublisherSession{}, err
		}
		raw := make([]byte, 32)
		if _, err := rand.Read(raw); err != nil {
			return schedulersdk.DefinitionPublisherSession{}, fmt.Errorf("generate Scheduler definition publisher nonce: %w", err)
		}
		nonce := base64.RawURLEncoding.EncodeToString(raw)
		digest := sha256.Sum256([]byte(nonce))
		hash := hex.EncodeToString(digest[:])
		now := time.Now().UTC().Format(time.RFC3339Nano)

		generation, found, err := s.activeGeneration(ctx)
		if err != nil {
			return schedulersdk.DefinitionPublisherSession{}, err
		}
		if !found {
			statement, args, buildErr := query.NewInsertBuilder(s.dialect, "_scheduler_definition_publications").Columns(
				"source_kind", "source_id", "active_generation", "active_session_sha256", "cursor_generation", "cursor_session_sha256", "cursor_revision", "cursor_content_sha256", "updated_at",
			).Values(runtimeDefinitionSourceKind, s.sourceID, 1, hash, nil, nil, nil, nil, now).OnConflictDoNothing("source_kind", "source_id").Build()
			if buildErr != nil {
				return schedulersdk.DefinitionPublisherSession{}, buildErr
			}
			if _, insertErr := s.database.ExecContext(ctx, statement, args...); insertErr != nil {
				return schedulersdk.DefinitionPublisherSession{}, insertErr
			}
			// Some drivers may report a no-op conflict update as one affected row.
			// Confirm ownership from the persisted fence instead of interpreting
			// driver-specific row counts.
			active, activeFound, activeErr := s.ActiveDefinitionPublisherSession(ctx)
			if activeErr != nil {
				return schedulersdk.DefinitionPublisherSession{}, activeErr
			}
			if activeFound && active.Generation == 1 && active.SessionSHA256 == hash {
				return schedulersdk.DefinitionPublisherSession{ContractVersion: schedulersdk.DefinitionPublicationContractVersion, Generation: 1, SessionNonce: nonce}, nil
			}
			lastErr = errDefinitionPublicationCAS
			if err := waitDefinitionPublicationRetry(ctx, attempt); err != nil {
				return schedulersdk.DefinitionPublisherSession{}, err
			}
			continue
		}
		if generation >= maxDefinitionPublisherGeneration {
			return schedulersdk.DefinitionPublisherSession{}, fmt.Errorf("Scheduler definition publisher generation exhausted")
		}
		next := generation + 1
		statement, args, buildErr := query.NewUpdateBuilder(s.dialect, "_scheduler_definition_publications").
			Set("active_generation", next).Set("active_session_sha256", hash).Set("updated_at", now).
			Where(query.And(
				query.Equal("source_kind", runtimeDefinitionSourceKind), query.Equal("source_id", s.sourceID), query.Equal("active_generation", generation),
			)).Build()
		if buildErr != nil {
			return schedulersdk.DefinitionPublisherSession{}, buildErr
		}
		result, updateErr := s.database.ExecContext(ctx, statement, args...)
		if updateErr != nil {
			return schedulersdk.DefinitionPublisherSession{}, updateErr
		}
		affected, rowsErr := result.RowsAffected()
		if rowsErr != nil {
			return schedulersdk.DefinitionPublisherSession{}, rowsErr
		}
		if affected == 1 {
			return schedulersdk.DefinitionPublisherSession{ContractVersion: schedulersdk.DefinitionPublicationContractVersion, Generation: next, SessionNonce: nonce}, nil
		}
		lastErr = errDefinitionPublicationCAS
		if err := waitDefinitionPublicationRetry(ctx, attempt); err != nil {
			return schedulersdk.DefinitionPublisherSession{}, err
		}
	}
	return schedulersdk.DefinitionPublisherSession{}, lastErr
}

func waitDefinitionPublicationRetry(ctx context.Context, attempt int) error {
	delay := time.Duration(attempt+1) * time.Millisecond
	if delay > 10*time.Millisecond {
		delay = 10 * time.Millisecond
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s DefinitionStore) ActiveDefinitionPublisherSession(ctx context.Context) (schedulersdk.DefinitionPublisherFence, bool, error) {
	statement, args, err := query.NewSelectBuilder(s.dialect, "_scheduler_definition_publications").Columns(
		"active_generation", "active_session_sha256",
	).Where(query.And(query.Equal("source_kind", runtimeDefinitionSourceKind), query.Equal("source_id", s.sourceID))).Build()
	if err != nil {
		return schedulersdk.DefinitionPublisherFence{}, false, err
	}
	var generation uint64
	var hash string
	if err := s.database.QueryRowContext(ctx, statement, args...).Scan(&generation, &hash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return schedulersdk.DefinitionPublisherFence{}, false, nil
		}
		return schedulersdk.DefinitionPublisherFence{}, false, err
	}
	fence := schedulersdk.DefinitionPublisherFence{ContractVersion: schedulersdk.DefinitionPublicationContractVersion, Generation: generation, SessionSHA256: hash}
	if err := fence.Validate(); err != nil {
		return schedulersdk.DefinitionPublisherFence{}, false, fmt.Errorf("persisted Scheduler definition publisher fence: %w", err)
	}
	return fence, true, nil
}

func (s DefinitionStore) activeGeneration(ctx context.Context) (uint64, bool, error) {
	fence, found, err := s.ActiveDefinitionPublisherSession(ctx)
	return fence.Generation, found, err
}

func (s DefinitionStore) acceptFencedSnapshot(ctx context.Context, tx *sql.Tx, snapshot schedulerpersistence.DefinitionSnapshot, contentHash, now string) (schedulersdk.DefinitionSnapshotDisposition, error) {
	if snapshot.PublisherFence == nil {
		return "", schedulersdk.ErrDefinitionPublicationRequired
	}
	incoming := *snapshot.PublisherFence
	if err := incoming.Validate(); err != nil {
		return "", err
	}
	selectPublication := query.NewSelectBuilder(s.dialect, "_scheduler_definition_publications").Columns(
		"active_generation", "active_session_sha256", "cursor_generation", "cursor_session_sha256", "cursor_revision", "cursor_content_sha256",
	).Where(query.And(query.Equal("source_kind", runtimeDefinitionSourceKind), query.Equal("source_id", s.sourceID)))
	statement, args, err := selectPublication.Build()
	if err != nil {
		return "", err
	}
	var activeGeneration uint64
	var activeHash string
	var cursorGeneration sql.NullInt64
	var cursorHash sql.NullString
	var cursorRevision sql.NullInt64
	var cursorContent sql.NullString
	if err := tx.QueryRowContext(ctx, statement, args...).Scan(&activeGeneration, &activeHash, &cursorGeneration, &cursorHash, &cursorRevision, &cursorContent); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", schedulersdk.ErrDefinitionPublicationRequired
		}
		return "", err
	}
	active := schedulersdk.DefinitionPublisherFence{ContractVersion: schedulersdk.DefinitionPublicationContractVersion, Generation: activeGeneration, SessionSHA256: activeHash}
	if err := active.Validate(); err != nil {
		return "", fmt.Errorf("persisted Scheduler active definition publication fence: %w", err)
	}
	var current *schedulersdk.DefinitionSnapshotCursor
	if cursorGeneration.Valid || cursorHash.Valid || cursorRevision.Valid || cursorContent.Valid {
		if !cursorGeneration.Valid || cursorGeneration.Int64 <= 0 || !cursorHash.Valid || !cursorRevision.Valid || !cursorContent.Valid {
			return "", fmt.Errorf("persisted Scheduler definition publication cursor is incomplete")
		}
		current = &schedulersdk.DefinitionSnapshotCursor{
			Application:    schedulersdk.ApplicationRef{RuntimeID: s.sourceID},
			PublisherFence: schedulersdk.DefinitionPublisherFence{ContractVersion: schedulersdk.DefinitionPublicationContractVersion, Generation: uint64(cursorGeneration.Int64), SessionSHA256: cursorHash.String},
			Revision:       cursorRevision.Int64, ContentSHA256: cursorContent.String,
		}
		if err := current.Validate(); err != nil {
			return "", fmt.Errorf("persisted Scheduler definition publication cursor: %w", err)
		}
	}
	// EvaluateDefinitionSnapshot hashes a raw nonce. Persistence deliberately
	// receives only the derived fence, so apply the equivalent ordering checks
	// here without ever materializing nonce authority.
	next := schedulersdk.DefinitionSnapshotCursor{
		Application: schedulersdk.ApplicationRef{RuntimeID: s.sourceID}, PublisherFence: incoming,
		Revision: snapshot.Revision, ContentSHA256: contentHash,
	}
	if snapshot.Revision <= 0 {
		return "", fmt.Errorf("%w: revision must be positive", schedulersdk.ErrDefinitionSnapshotConflict)
	}
	if !incoming.Equal(active) {
		if incoming.Generation < active.Generation {
			return "", schedulersdk.ErrDefinitionSnapshotStale
		}
		return "", schedulersdk.ErrDefinitionPublicationSessionMismatch
	}
	if current != nil {
		if incoming.Generation < current.PublisherFence.Generation {
			return "", schedulersdk.ErrDefinitionSnapshotStale
		}
		if incoming.Generation == current.PublisherFence.Generation {
			if !incoming.Equal(current.PublisherFence) {
				return "", schedulersdk.ErrDefinitionSnapshotConflict
			}
			if snapshot.Revision < current.Revision {
				return "", schedulersdk.ErrDefinitionSnapshotStale
			}
			if snapshot.Revision == current.Revision {
				if contentHash != current.ContentSHA256 {
					return "", schedulersdk.ErrDefinitionSnapshotConflict
				}
				return schedulersdk.DefinitionSnapshotReplay, nil
			}
		}
	}
	predicates := []query.Predicate{
		query.Equal("source_kind", runtimeDefinitionSourceKind), query.Equal("source_id", s.sourceID),
		query.Equal("active_generation", active.Generation), query.Equal("active_session_sha256", active.SessionSHA256),
	}
	if current == nil {
		predicates = append(predicates, query.IsNull("cursor_generation"), query.IsNull("cursor_session_sha256"), query.IsNull("cursor_revision"), query.IsNull("cursor_content_sha256"))
	} else {
		predicates = append(predicates,
			query.Equal("cursor_generation", current.PublisherFence.Generation), query.Equal("cursor_session_sha256", current.PublisherFence.SessionSHA256),
			query.Equal("cursor_revision", current.Revision), query.Equal("cursor_content_sha256", current.ContentSHA256),
		)
	}
	update, updateArgs, err := query.NewUpdateBuilder(s.dialect, "_scheduler_definition_publications").
		Set("cursor_generation", next.PublisherFence.Generation).Set("cursor_session_sha256", next.PublisherFence.SessionSHA256).
		Set("cursor_revision", next.Revision).Set("cursor_content_sha256", next.ContentSHA256).Set("updated_at", now).
		Where(query.And(predicates...)).Build()
	if err != nil {
		return "", err
	}
	result, err := tx.ExecContext(ctx, update, updateArgs...)
	if err != nil {
		return "", err
	}
	updated, err := result.RowsAffected()
	if err != nil || updated != 1 {
		if err != nil {
			return "", err
		}
		return "", errDefinitionPublicationCAS
	}
	return schedulersdk.DefinitionSnapshotApply, nil
}

func (s DefinitionStore) definitionCursorFrom(ctx context.Context, source modulehost.Queryer) (*schedulersdk.DefinitionSnapshotCursor, error) {
	statement, args, err := query.NewSelectBuilder(s.dialect, "_scheduler_definition_publications").Columns(
		"cursor_generation", "cursor_session_sha256", "cursor_revision", "cursor_content_sha256",
	).Where(query.And(query.Equal("source_kind", runtimeDefinitionSourceKind), query.Equal("source_id", s.sourceID))).Build()
	if err != nil {
		return nil, err
	}
	var generation sql.NullInt64
	var hash sql.NullString
	var revision sql.NullInt64
	var content sql.NullString
	if err := source.QueryRowContext(ctx, statement, args...).Scan(&generation, &hash, &revision, &content); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if !generation.Valid && !hash.Valid && !revision.Valid && !content.Valid {
		return nil, nil
	}
	if !generation.Valid || generation.Int64 <= 0 || !hash.Valid || !revision.Valid || !content.Valid {
		return nil, fmt.Errorf("persisted Scheduler definition publication cursor is incomplete")
	}
	return &schedulersdk.DefinitionSnapshotCursor{
		Application:    schedulersdk.ApplicationRef{RuntimeID: s.sourceID},
		PublisherFence: schedulersdk.DefinitionPublisherFence{ContractVersion: schedulersdk.DefinitionPublicationContractVersion, Generation: uint64(generation.Int64), SessionSHA256: hash.String},
		Revision:       revision.Int64, ContentSHA256: content.String,
	}, nil
}

func (s DefinitionStore) encodeDefinitions(definitions []schedulersdk.Definition) ([]encodedDefinition, string, error) {
	values := make([]encodedDefinition, 0, len(definitions))
	seen := make(map[string]bool, len(definitions))
	for _, definition := range definitions {
		definition = definition.Normalize()
		if err := definition.Validate(); err != nil {
			return nil, "", err
		}
		key := strings.TrimSpace(definition.Key)
		if seen[key] {
			return nil, "", fmt.Errorf("Scheduler definition snapshot repeats %q", key)
		}
		seen[key] = true
		raw, err := json.Marshal(definition)
		if err != nil {
			return nil, "", err
		}
		sum := sha256.Sum256(raw)
		values = append(values, encodedDefinition{definition: definition, raw: raw, hash: hex.EncodeToString(sum[:]), storageKey: definitionStorageKey(s.sourceID, key)})
	}
	sort.Slice(values, func(i, j int) bool { return values[i].definition.Key < values[j].definition.Key })
	canonical := make([]schedulersdk.Definition, len(values))
	for index := range values {
		canonical[index] = values[index].definition
	}
	raw, err := json.Marshal(canonical)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(raw)
	return values, hex.EncodeToString(sum[:]), nil
}

func (s DefinitionStore) writeSnapshotState(ctx context.Context, tx *sql.Tx, snapshot schedulerpersistence.DefinitionSnapshot, snapshotHash, now string) error {
	update, values, err := query.NewUpdateBuilder(s.dialect, "_scheduler_definition_snapshots").
		Set("revision", snapshot.Revision).Set("schema_version", strings.TrimSpace(snapshot.SchemaVersion)).Set("schema_hash", snapshotHash).Set("updated_at", now).
		Where(query.And(query.Equal("source_kind", runtimeDefinitionSourceKind), query.Equal("source_id", s.sourceID))).Build()
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, update, values...)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 1 {
		return nil
	}
	insert, values, err := query.NewInsertBuilder(s.dialect, "_scheduler_definition_snapshots").Columns(
		"source_kind", "source_id", "revision", "schema_version", "schema_hash", "updated_at",
	).Values(runtimeDefinitionSourceKind, s.sourceID, snapshot.Revision, strings.TrimSpace(snapshot.SchemaVersion), snapshotHash, now).Build()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, insert, values...)
	return err
}

func (s DefinitionStore) DefinitionSnapshot(ctx context.Context) (schedulerpersistence.DefinitionSnapshot, error) {
	if s.database == nil || s.dialect == nil {
		return schedulerpersistence.DefinitionSnapshot{}, fmt.Errorf("Scheduler definition store is unavailable")
	}
	tx, err := s.database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return schedulerpersistence.DefinitionSnapshot{}, err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := s.definitionSnapshotFrom(ctx, tx)
	if err != nil {
		return schedulerpersistence.DefinitionSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return schedulerpersistence.DefinitionSnapshot{}, err
	}
	return result, nil
}

func (s DefinitionStore) definitionSnapshotFrom(ctx context.Context, source modulehost.Queryer) (schedulerpersistence.DefinitionSnapshot, error) {
	result := schedulerpersistence.DefinitionSnapshot{SourceKind: runtimeDefinitionSourceKind, SourceID: s.sourceID, Definitions: []schedulersdk.Definition{}}
	stateQuery, stateArgs, err := query.NewSelectBuilder(s.dialect, "_scheduler_definition_snapshots").Columns(
		"revision", "schema_version", "schema_hash",
	).Where(query.And(query.Equal("source_kind", runtimeDefinitionSourceKind), query.Equal("source_id", s.sourceID))).Build()
	if err != nil {
		return schedulerpersistence.DefinitionSnapshot{}, err
	}
	hasGeneration := true
	if err := source.QueryRowContext(ctx, stateQuery, stateArgs...).Scan(&result.Revision, &result.SchemaVersion, &result.SchemaHash); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return schedulerpersistence.DefinitionSnapshot{}, err
		}
		hasGeneration = false
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_scheduler_definitions").Columns(
		"payload_json", "schema_version",
	).Where(query.And(
		query.Equal("source_kind", runtimeDefinitionSourceKind), query.Equal("source_id", s.sourceID), query.IsNull("disabled_at"),
	)).OrderBy(query.Ascending("object_key")).Build()
	if err != nil {
		return schedulerpersistence.DefinitionSnapshot{}, err
	}
	rows, err := source.QueryContext(ctx, statement, args...)
	if err != nil {
		return schedulerpersistence.DefinitionSnapshot{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		var schemaVersion string
		if err := rows.Scan(&raw, &schemaVersion); err != nil {
			return schedulerpersistence.DefinitionSnapshot{}, err
		}
		var definition schedulersdk.Definition
		if err := json.Unmarshal(raw, &definition); err != nil {
			return schedulerpersistence.DefinitionSnapshot{}, err
		}
		result.Definitions = append(result.Definitions, definition)
		if !hasGeneration {
			schemaVersion = strings.TrimSpace(schemaVersion)
			if result.SchemaVersion != "" && result.SchemaVersion != schemaVersion {
				return schedulerpersistence.DefinitionSnapshot{}, fmt.Errorf("legacy Scheduler definition snapshot has mixed schema versions")
			}
			result.SchemaVersion = schemaVersion
		}
	}
	if err := rows.Err(); err != nil {
		return schedulerpersistence.DefinitionSnapshot{}, err
	}
	if err := rows.Close(); err != nil {
		return schedulerpersistence.DefinitionSnapshot{}, err
	}
	sort.Slice(result.Definitions, func(i, j int) bool { return result.Definitions[i].Key < result.Definitions[j].Key })
	if !hasGeneration && len(result.Definitions) != 0 {
		revision, err := strconv.ParseInt(result.SchemaVersion, 10, 64)
		if err != nil || revision < 0 {
			return schedulerpersistence.DefinitionSnapshot{}, fmt.Errorf("legacy Scheduler definition snapshot revision is invalid")
		}
		result.Revision = revision
		raw, err := json.Marshal(result.Definitions)
		if err != nil {
			return schedulerpersistence.DefinitionSnapshot{}, err
		}
		sum := sha256.Sum256(raw)
		result.SchemaHash = hex.EncodeToString(sum[:])
	}
	_, contentHash, hashErr := s.encodeDefinitions(result.Definitions)
	if hashErr != nil {
		return schedulerpersistence.DefinitionSnapshot{}, hashErr
	}
	if hasGeneration && result.SchemaHash != contentHash {
		return schedulerpersistence.DefinitionSnapshot{}, fmt.Errorf("persisted Scheduler definition snapshot content is inconsistent")
	}
	if cursor, err := s.definitionCursorFrom(ctx, source); err != nil {
		return schedulerpersistence.DefinitionSnapshot{}, err
	} else if cursor != nil {
		if cursor.Revision != result.Revision || cursor.ContentSHA256 != contentHash {
			return schedulerpersistence.DefinitionSnapshot{}, fmt.Errorf("persisted Scheduler fenced definition snapshot is inconsistent")
		}
		fence := cursor.PublisherFence
		result.PublisherFence = &fence
		result.ContentSHA256 = cursor.ContentSHA256
	}
	return result, nil
}

// definitionStorageKey preserves the existing table's global resource_key
// uniqueness while making the persisted identity application-scoped. This
// avoids a dialect-specific destructive constraint rewrite; legacy unscoped
// rows are disabled by the first bound sync and replaced transactionally.
func definitionStorageKey(runtimeID, definitionKey string) string {
	sum := sha256.Sum256([]byte(runtimeDefinitionSourceKind + "\x00" + strings.TrimSpace(runtimeID) + "\x00" + strings.TrimSpace(definitionKey)))
	return hex.EncodeToString(sum[:])
}

var _ schedulerpersistence.DefinitionRepository = DefinitionStore{}
var _ schedulerpersistence.DefinitionPublicationRepository = DefinitionStore{}
