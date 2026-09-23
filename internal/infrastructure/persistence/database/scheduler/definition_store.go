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
	"strings"
	"time"

	"github.com/domainry/domainry-foundation/mutation"
	metadatasdk "github.com/domainry/domainry-metadata-sdk"
	metadatamodulehost "github.com/domainry/domainry-metadata-sdk/modulehost"
	"github.com/domainry/domainry-orm/query"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulerpersistence "github.com/domainry/domainry-scheduler-sdk/persistence"
)

const (
	runtimeDefinitionSourceKind         = "runtime_host"
	sharedSchedulerDefinitionType       = "scheduler"
	sharedSchedulerStateContractVersion = "domainry-scheduler-definition-state-v1"
	definitionProjectionScheduleID      = "__scheduler_definition_projection__"
	definitionProjectionKind            = "definition_projection"
	maxDefinitionPublisherGeneration    = 1<<53 - 1
)

var errDefinitionPublicationCAS = errors.New("Scheduler definition publication compare-and-swap lost")

// sharedDefinitionState is the one typed, versioned Scheduler definition
// aggregate stored in shared _definitions/_definition_versions. It replaces
// the private current-definition, snapshot and publication tables together so
// the active publisher fence and canonical payload advance under one CAS.
type sharedDefinitionState struct {
	ContractVersion string                                 `json:"contract_version"`
	Revision        int64                                  `json:"revision,omitempty"`
	ContentSHA256   string                                 `json:"content_sha256,omitempty"`
	SchemaVersion   string                                 `json:"schema_version,omitempty"`
	SchemaHash      string                                 `json:"schema_hash,omitempty"`
	SourceKind      string                                 `json:"source_kind"`
	SourceID        string                                 `json:"source_id"`
	Definitions     []schedulersdk.Definition              `json:"definitions"`
	ActivePublisher *schedulersdk.DefinitionPublisherFence `json:"active_publisher,omitempty"`
	Cursor          *schedulersdk.DefinitionSnapshotCursor `json:"cursor,omitempty"`
}

type DefinitionStore struct {
	database modulehost.Database
	dialect  modulehost.Dialect
	shared   metadatasdk.DefinitionStore
	sourceID string
	mode     schedulersdk.DeploymentMode
}

func NewDefinitionStore(database modulehost.Database, dialect modulehost.Dialect, shared metadatasdk.DefinitionStore, runtimeID string, mode schedulersdk.DeploymentMode) (DefinitionStore, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if database == nil || dialect == nil || shared == nil {
		return DefinitionStore{}, fmt.Errorf("Scheduler database, dialect, and shared Definition store are required")
	}
	if runtimeID == "" {
		return DefinitionStore{}, fmt.Errorf("Scheduler definition store Runtime identity is required")
	}
	if mode != schedulersdk.DeploymentModeModule && mode != schedulersdk.DeploymentModeSaaS {
		return DefinitionStore{}, fmt.Errorf("Scheduler definition store deployment mode is required")
	}
	return DefinitionStore{database: database, dialect: dialect, shared: shared, sourceID: runtimeID, mode: mode}, nil
}

func (s DefinitionStore) SyncDefinitions(ctx context.Context, snapshot schedulerpersistence.DefinitionSnapshot) error {
	if snapshot.Revision <= 0 || strings.TrimSpace(snapshot.SchemaVersion) == "" || strings.TrimSpace(snapshot.SourceKind) == "" || strings.TrimSpace(snapshot.SourceID) == "" {
		return fmt.Errorf("Scheduler definition snapshot identity is required")
	}
	if strings.TrimSpace(snapshot.SourceKind) != runtimeDefinitionSourceKind || strings.TrimSpace(snapshot.SourceID) != s.sourceID {
		return fmt.Errorf("Scheduler definition snapshot does not match the bound Runtime application")
	}
	definitions, contentHash, err := canonicalDefinitions(snapshot.Definitions)
	if err != nil {
		return err
	}
	if snapshot.ContentSHA256 != "" && snapshot.ContentSHA256 != contentHash {
		return fmt.Errorf("Scheduler definition snapshot content hash does not match its definitions")
	}
	switch s.mode {
	case schedulersdk.DeploymentModeModule:
		if snapshot.PublisherFence != nil {
			return fmt.Errorf("Scheduler Module definition snapshot must not carry a publisher fence")
		}
		return s.updateState(ctx, func(state *sharedDefinitionState) (bool, error) {
			state.Revision = snapshot.Revision
			state.ContentSHA256 = contentHash
			state.SchemaVersion = strings.TrimSpace(snapshot.SchemaVersion)
			state.SchemaHash = contentHash
			state.Definitions = definitions
			state.ActivePublisher = nil
			state.Cursor = nil
			return true, nil
		})
	case schedulersdk.DeploymentModeSaaS:
		if snapshot.PublisherFence == nil {
			return schedulersdk.ErrDefinitionPublicationRequired
		}
		incoming := *snapshot.PublisherFence
		if err := incoming.Validate(); err != nil {
			return err
		}
		return s.updateState(ctx, func(state *sharedDefinitionState) (bool, error) {
			if state.ActivePublisher == nil {
				return false, schedulersdk.ErrDefinitionPublicationRequired
			}
			active := *state.ActivePublisher
			if !incoming.Equal(active) {
				if incoming.Generation < active.Generation {
					return false, schedulersdk.ErrDefinitionSnapshotStale
				}
				return false, schedulersdk.ErrDefinitionPublicationSessionMismatch
			}
			if state.Cursor != nil {
				current := *state.Cursor
				if incoming.Generation < current.PublisherFence.Generation {
					return false, schedulersdk.ErrDefinitionSnapshotStale
				}
				if incoming.Generation == current.PublisherFence.Generation {
					if !incoming.Equal(current.PublisherFence) {
						return false, schedulersdk.ErrDefinitionSnapshotConflict
					}
					if snapshot.Revision < current.Revision {
						return false, schedulersdk.ErrDefinitionSnapshotStale
					}
					if snapshot.Revision == current.Revision {
						if contentHash != current.ContentSHA256 {
							return false, schedulersdk.ErrDefinitionSnapshotConflict
						}
						return false, nil
					}
				}
			}
			state.Revision = snapshot.Revision
			state.ContentSHA256 = contentHash
			state.SchemaVersion = strings.TrimSpace(snapshot.SchemaVersion)
			state.SchemaHash = contentHash
			state.Definitions = definitions
			state.Cursor = &schedulersdk.DefinitionSnapshotCursor{
				Application: schedulersdk.ApplicationRef{RuntimeID: s.sourceID}, PublisherFence: incoming,
				Revision: snapshot.Revision, ContentSHA256: contentHash,
			}
			return true, nil
		})
	default:
		return fmt.Errorf("Scheduler definition store deployment mode is invalid")
	}
}

func (s DefinitionStore) BeginDefinitionPublisherSession(ctx context.Context) (schedulersdk.DefinitionPublisherSession, error) {
	if s.mode != schedulersdk.DeploymentModeSaaS {
		return schedulersdk.DefinitionPublisherSession{}, schedulersdk.ErrDefinitionPublicationCapabilityRequired
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return schedulersdk.DefinitionPublisherSession{}, fmt.Errorf("generate Scheduler definition publisher nonce: %w", err)
	}
	nonce := base64.RawURLEncoding.EncodeToString(raw)
	digest := sha256.Sum256([]byte(nonce))
	hash := hex.EncodeToString(digest[:])
	var generation uint64
	err := s.updateState(ctx, func(state *sharedDefinitionState) (bool, error) {
		if state.ActivePublisher != nil {
			generation = state.ActivePublisher.Generation
		}
		if generation >= maxDefinitionPublisherGeneration {
			return false, fmt.Errorf("Scheduler definition publisher generation exhausted")
		}
		generation++
		state.ActivePublisher = &schedulersdk.DefinitionPublisherFence{
			ContractVersion: schedulersdk.DefinitionPublicationContractVersion,
			Generation:      generation, SessionSHA256: hash,
		}
		return true, nil
	})
	if err != nil {
		return schedulersdk.DefinitionPublisherSession{}, err
	}
	return schedulersdk.DefinitionPublisherSession{
		ContractVersion: schedulersdk.DefinitionPublicationContractVersion,
		Generation:      generation, SessionNonce: nonce,
	}, nil
}

func (s DefinitionStore) ActiveDefinitionPublisherSession(ctx context.Context) (schedulersdk.DefinitionPublisherFence, bool, error) {
	state, _, found, err := s.loadState(ctx)
	if err != nil || !found || state.ActivePublisher == nil {
		return schedulersdk.DefinitionPublisherFence{}, false, err
	}
	return *state.ActivePublisher, true, nil
}

func (s DefinitionStore) DefinitionSnapshot(ctx context.Context) (schedulerpersistence.DefinitionSnapshot, error) {
	state, _, found, err := s.loadState(ctx)
	if err != nil {
		return schedulerpersistence.DefinitionSnapshot{}, err
	}
	result := schedulerpersistence.DefinitionSnapshot{SourceKind: runtimeDefinitionSourceKind, SourceID: s.sourceID, Definitions: []schedulersdk.Definition{}}
	if !found {
		return result, nil
	}
	result.Revision = state.Revision
	result.SchemaVersion = state.SchemaVersion
	result.SchemaHash = state.SchemaHash
	result.Definitions = append(result.Definitions, state.Definitions...)
	if state.Cursor != nil {
		fence := state.Cursor.PublisherFence
		result.PublisherFence = &fence
		result.ContentSHA256 = state.Cursor.ContentSHA256
	}
	return result, nil
}

func (s DefinitionStore) updateState(ctx context.Context, mutate func(*sharedDefinitionState) (bool, error)) error {
	var lastErr error
	for attempt := 0; attempt < 64; attempt++ {
		state, versionID, found, err := s.loadState(ctx)
		if err != nil {
			return err
		}
		changed, err := mutate(&state)
		if err != nil || !changed {
			return err
		}
		expected := versionID
		if !found {
			expected = metadatasdk.DefinitionNoCurrentVersion
		}
		if err := s.publishState(ctx, expected, state); err == nil {
			return nil
		} else if !definitionPublicationRetryable(err) {
			return err
		} else {
			lastErr = err
		}
		if err := waitDefinitionPublicationRetry(ctx, attempt); err != nil {
			return err
		}
	}
	return lastErr
}

func (s DefinitionStore) loadState(ctx context.Context) (sharedDefinitionState, string, bool, error) {
	state := sharedDefinitionState{
		ContractVersion: sharedSchedulerStateContractVersion,
		SourceKind:      runtimeDefinitionSourceKind, SourceID: s.sourceID,
		Definitions: []schedulersdk.Definition{},
	}
	definition, found, err := s.shared.Get(ctx, metadatasdk.DefinitionOwnerScheduler, sharedSchedulerDefinitionType, s.sourceID)
	if err != nil || !found {
		return state, "", found, err
	}
	if err := json.Unmarshal(definition.Payload, &state); err != nil {
		return sharedDefinitionState{}, "", false, fmt.Errorf("decode shared Scheduler definition state: %w", err)
	}
	if err := s.validateState(&state); err != nil {
		return sharedDefinitionState{}, "", false, err
	}
	return state, definition.CurrentVersionID, true, nil
}

func (s DefinitionStore) validateState(state *sharedDefinitionState) error {
	if state == nil || state.ContractVersion != sharedSchedulerStateContractVersion || strings.TrimSpace(state.SourceKind) != runtimeDefinitionSourceKind || strings.TrimSpace(state.SourceID) != s.sourceID {
		return fmt.Errorf("persisted Scheduler definition state identity is inconsistent")
	}
	definitions, contentHash, err := canonicalDefinitions(state.Definitions)
	if err != nil {
		return err
	}
	state.Definitions = definitions
	if state.Revision > 0 && (strings.TrimSpace(state.SchemaVersion) == "" || state.ContentSHA256 != contentHash || state.SchemaHash != contentHash) {
		return fmt.Errorf("persisted Scheduler definition snapshot content is inconsistent")
	}
	if state.ActivePublisher != nil {
		if err := state.ActivePublisher.Validate(); err != nil {
			return fmt.Errorf("persisted Scheduler active definition publication fence: %w", err)
		}
	}
	if state.Cursor != nil {
		if err := state.Cursor.Validate(); err != nil {
			return fmt.Errorf("persisted Scheduler definition publication cursor: %w", err)
		}
		if strings.TrimSpace(state.Cursor.Application.RuntimeID) != s.sourceID || state.Cursor.Revision != state.Revision || state.Cursor.ContentSHA256 != state.ContentSHA256 {
			return fmt.Errorf("persisted Scheduler fenced definition snapshot is inconsistent")
		}
	}
	return nil
}

func (s DefinitionStore) publishState(ctx context.Context, expectedVersionID string, state sharedDefinitionState) error {
	state.ContractVersion = sharedSchedulerStateContractVersion
	state.SourceKind = runtimeDefinitionSourceKind
	state.SourceID = s.sourceID
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(payload)
	hash := hex.EncodeToString(digest[:])
	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	sharedCtx := metadatamodulehost.WithExecutor(ctx, tx)
	_, err = s.shared.Publish(sharedCtx, metadatasdk.DefinitionPublishCommand{
		Owner: metadatasdk.DefinitionOwnerScheduler, ResourceType: sharedSchedulerDefinitionType, ResourceKey: s.sourceID,
		ExpectedCurrentVersionID: expectedVersionID, SchemaVersion: "scheduler-state:" + hash, SchemaHash: hash,
		ObjectKey: s.sourceID, Name: "Scheduler definition state " + s.sourceID, Payload: payload,
		SourceKind: runtimeDefinitionSourceKind, SourceID: s.sourceID, PublishedBy: "system:scheduler:" + s.sourceID,
	})
	if err != nil {
		return err
	}
	if state.Cursor != nil {
		if err := s.storeProjectionCursor(ctx, tx, *state.Cursor); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s DefinitionStore) storeProjectionCursor(ctx context.Context, tx *sql.Tx, cursor schedulersdk.DefinitionSnapshotCursor) error {
	if err := cursor.Validate(); err != nil {
		return err
	}
	now := formatTime(time.Now().UTC())
	insert, args, err := query.NewInsertBuilder(s.dialect, "_scheduler_schedules").Columns(
		"runtime_id", "schedule_id", "kind", "source_kind", "source_id", "enabled", "snapshot_revision",
		"publication_generation", "publication_session_sha256", "publication_content_sha256", "created_at", "updated_at",
	).Values(
		s.sourceID, definitionProjectionScheduleID, definitionProjectionKind, runtimeDefinitionSourceKind, s.sourceID, false,
		cursor.Revision, cursor.PublisherFence.Generation, cursor.PublisherFence.SessionSHA256, cursor.ContentSHA256, now, now,
	).OnConflictDoNothing("runtime_id", "schedule_id").Build()
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, insert, args...); err != nil {
		return err
	}
	update, args, err := query.NewUpdateBuilder(s.dialect, "_scheduler_schedules").
		Set("snapshot_revision", cursor.Revision).Set("publication_generation", cursor.PublisherFence.Generation).
		Set("publication_session_sha256", cursor.PublisherFence.SessionSHA256).Set("publication_content_sha256", cursor.ContentSHA256).
		Set("updated_at", now).Where(query.And(
		query.Equal("runtime_id", s.sourceID), query.Equal("schedule_id", definitionProjectionScheduleID), query.Equal("kind", definitionProjectionKind),
	)).Build()
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, update, args...)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		if err != nil {
			return err
		}
		return errDefinitionPublicationCAS
	}
	return nil
}

func canonicalDefinitions(definitions []schedulersdk.Definition) ([]schedulersdk.Definition, string, error) {
	canonical := make([]schedulersdk.Definition, len(definitions))
	copy(canonical, definitions)
	seen := make(map[string]bool, len(canonical))
	for index := range canonical {
		canonical[index] = canonical[index].Normalize()
		if err := canonical[index].Validate(); err != nil {
			return nil, "", err
		}
		key := strings.TrimSpace(canonical[index].Key)
		if seen[key] {
			return nil, "", fmt.Errorf("Scheduler definition snapshot repeats %q", key)
		}
		seen[key] = true
	}
	sort.Slice(canonical, func(i, j int) bool { return canonical[i].Key < canonical[j].Key })
	hash, err := schedulersdk.DefinitionSnapshotContentSHA256(canonical)
	return canonical, hash, err
}

func metadataCASConflict(err error) bool {
	var metadataErr *metadatasdk.Error
	return errors.As(err, &metadataErr) && metadataErr.StatusCode == 409
}

func definitionPublicationRetryable(err error) bool {
	if metadataCASConflict(err) || errors.Is(err, errDefinitionPublicationCAS) {
		return true
	}
	// Retrying the whole shared-definition CAS is safe because transient
	// transaction failures surface before either participating write commits.
	classified := mutation.TransactionError(err, "scheduler_definition_state", "publication")
	return mutation.IsTransactionTransient(classified, "")
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

var _ schedulerpersistence.DefinitionRepository = DefinitionStore{}
var _ schedulerpersistence.DefinitionPublicationRepository = DefinitionStore{}
