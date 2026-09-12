package scheduler

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/domainry/domainry-orm/query"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulerpersistence "github.com/domainry/domainry-scheduler-sdk/persistence"
	"github.com/domainry/domainry-scheduler-sdk/schedule"
)

type Store struct {
	db                  modulehost.Database
	dialect             modulehost.Dialect
	runtimeID, workerID string
}

const (
	definitionStateSourceRuntime = "runtime_definition"
	definitionStateSourcePlan    = "scheduled_plan"
)

type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func New(db modulehost.Database, dialect modulehost.Dialect, runtimeID, workerID string) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("Scheduler database is required")
	}
	if dialect == nil {
		return nil, fmt.Errorf("Scheduler database dialect is required")
	}
	runtimeID, workerID = strings.TrimSpace(runtimeID), strings.TrimSpace(workerID)
	if runtimeID == "" || workerID == "" {
		return nil, fmt.Errorf("Scheduler runtime and worker identity are required")
	}
	return &Store{db: db, dialect: dialect, runtimeID: runtimeID, workerID: workerID}, nil
}

func (s *Store) Reconcile(ctx context.Context, d schedulersdk.Definition, next time.Time) error {
	return s.reconcileWith(ctx, s.db, d, next, definitionStateSourceRuntime, true)
}

func (s *Store) ReconcileScheduledPlan(ctx context.Context, d schedulersdk.Definition, next time.Time, enabled bool) error {
	return s.reconcileWith(ctx, s.db, d, next, definitionStateSourcePlan, enabled)
}

func (s *Store) reconcileWith(ctx context.Context, executor sqlExecutor, d schedulersdk.Definition, next time.Time, sourceKind string, enabled bool) error {
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	now := formatTime(time.Now())
	predicate := s.definitionSourceIdentity(d.Key, sourceKind)

	changed, changedArgs, err := query.NewUpdateBuilder(s.dialect, "_scheduler_definition_states").
		Set("revision", d.Revision).Set("enabled", enabled).Set("definition_json", string(raw)).Set("next_run_at", formatTime(next)).Set("updated_at", now).
		Where(query.And(predicate, query.NotEqual("revision", d.Revision))).Build()
	if err != nil {
		return err
	}
	result, err := executor.ExecContext(ctx, changed, changedArgs...)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected > 0 {
		return nil
	}
	stableBuilder := query.NewUpdateBuilder(s.dialect, "_scheduler_definition_states").Set("definition_json", string(raw)).Set("updated_at", now).Where(predicate)
	// A completed one-time plan disables its execution state. Rehydrating the
	// same immutable plan revision must preserve that terminal cursor instead
	// of turning every restart into another due scan. Explicit plan management
	// changes revision before changing enabled state.
	if sourceKind != definitionStateSourcePlan || !enabled {
		stableBuilder.Set("enabled", enabled)
	}
	stable, stableArgs, err := stableBuilder.Build()
	if err != nil {
		return err
	}
	result, err = executor.ExecContext(ctx, stable, stableArgs...)
	if err != nil {
		return err
	}
	affected, _ = result.RowsAffected()
	if affected > 0 {
		return nil
	}
	insert, insertArgs, err := query.NewInsertBuilder(s.dialect, "_scheduler_definition_states").Columns("runtime_id", "definition_key", "revision", "enabled", "definition_json", "next_run_at", "snapshot_revision", "updated_at", "source_kind").Values(s.runtimeID, d.Key, d.Revision, enabled, string(raw), formatTime(next), int64(0), now, sourceKind).Build()
	if err != nil {
		return err
	}
	_, err = executor.ExecContext(ctx, insert, insertArgs...)
	if err == nil {
		return nil
	}
	if !isUnique(err) {
		return err
	}
	result, err = executor.ExecContext(ctx, stable, stableArgs...)
	if err != nil {
		return err
	}
	affected, _ = result.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("Scheduler definition key %q conflicts with another state source", d.Key)
	}
	return nil
}

func (s *Store) DisableMissing(ctx context.Context, active []string, revision int64) error {
	return s.disableMissingWith(ctx, s.db, active, revision)
}

func (s *Store) disableMissingWith(ctx context.Context, executor sqlExecutor, active []string, revision int64) error {
	update, args, err := query.NewUpdateBuilder(s.dialect, "_scheduler_definition_states").Set("enabled", false).Set("snapshot_revision", revision).Where(query.And(query.Equal("runtime_id", s.runtimeID), query.Equal("source_kind", definitionStateSourceRuntime))).Build()
	if err != nil {
		return err
	}
	if _, err = executor.ExecContext(ctx, update, args...); err != nil {
		return err
	}
	if len(active) == 0 {
		return nil
	}
	values := make([]any, len(active))
	for i, key := range active {
		values[i] = key
	}
	update, args, err = query.NewUpdateBuilder(s.dialect, "_scheduler_definition_states").Set("enabled", true).Set("snapshot_revision", revision).Where(query.And(query.Equal("runtime_id", s.runtimeID), query.Equal("source_kind", definitionStateSourceRuntime), query.In("definition_key", values...))).Build()
	if err != nil {
		return err
	}
	_, err = executor.ExecContext(ctx, update, args...)
	return err
}

// ApplyDefinitionSnapshot projects one fenced canonical SaaS snapshot into the
// execution state while holding the canonical publication cursor row. A stale
// reader therefore either applies before the newer canonical commit or observes
// the new cursor and performs no writes; it cannot regress the shared worker
// projection after a newer snapshot has committed.
func (s *Store) ApplyDefinitionSnapshot(ctx context.Context, snapshot schedulerpersistence.DefinitionSnapshot, now time.Time) (bool, error) {
	if snapshot.PublisherFence == nil || snapshot.Revision <= 0 || strings.TrimSpace(snapshot.ContentSHA256) == "" {
		return false, fmt.Errorf("Scheduler fenced definition projection identity is required")
	}
	if err := snapshot.PublisherFence.Validate(); err != nil {
		return false, err
	}
	if strings.TrimSpace(snapshot.SourceKind) != runtimeDefinitionSourceKind || strings.TrimSpace(snapshot.SourceID) != s.runtimeID {
		return false, fmt.Errorf("Scheduler fenced definition projection does not match the bound Runtime application")
	}
	contentHash, err := schedulersdk.DefinitionSnapshotContentSHA256(snapshot.Definitions)
	if err != nil {
		return false, err
	}
	if contentHash != snapshot.ContentSHA256 {
		return false, fmt.Errorf("Scheduler fenced definition projection content hash is inconsistent")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := s.lockDefinitionSnapshotCursor(ctx, tx, snapshot)
	if err != nil || !current {
		return false, err
	}
	active := make([]string, 0, len(snapshot.Definitions))
	for _, definition := range snapshot.Definitions {
		definition = definition.Normalize()
		if err := definition.Validate(); err != nil {
			return false, err
		}
		if err := schedule.Validate(definition.Schedule); err != nil {
			return false, fmt.Errorf("scheduler definition %s: %w", definition.Key, err)
		}
		if !strings.EqualFold(strings.TrimSpace(definition.Status), "enabled") {
			continue
		}
		active = append(active, definition.Key)
		next := schedule.NextSchedule(definition.Schedule, now)
		if !definition.InitialNextRunAt.IsZero() && definition.InitialNextRunAt.Before(next) {
			next = definition.InitialNextRunAt.UTC()
		}
		if err := s.reconcileWith(ctx, tx, definition, next, definitionStateSourceRuntime, true); err != nil {
			return false, err
		}
	}
	if err := s.disableMissingWith(ctx, tx, active, snapshot.Revision); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) lockDefinitionSnapshotCursor(ctx context.Context, tx *sql.Tx, snapshot schedulerpersistence.DefinitionSnapshot) (bool, error) {
	fence := *snapshot.PublisherFence
	// Match the published cursor, not the active session. Begin may already have
	// retired the cursor's session without publishing replacement content; until
	// that publication commits, the prior canonical snapshot remains executable.
	predicate := query.And(
		query.Equal("source_kind", runtimeDefinitionSourceKind), query.Equal("source_id", s.runtimeID),
		query.Equal("cursor_generation", fence.Generation), query.Equal("cursor_session_sha256", fence.SessionSHA256),
		query.Equal("cursor_revision", snapshot.Revision), query.Equal("cursor_content_sha256", snapshot.ContentSHA256),
	)
	// The deliberately idempotent update is a cross-dialect write lock. Do not
	// infer a match from RowsAffected: driver configurations disagree for no-op
	// updates, so the cursor is read back and compared inside this transaction.
	lockStatement, lockArgs, err := query.NewUpdateBuilder(s.dialect, "_scheduler_definition_publications").
		Set("cursor_revision", snapshot.Revision).Where(predicate).Build()
	if err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, lockStatement, lockArgs...); err != nil {
		return false, err
	}
	selectStatement, selectArgs, err := query.NewSelectBuilder(s.dialect, "_scheduler_definition_publications").Columns(
		"cursor_generation", "cursor_session_sha256", "cursor_revision", "cursor_content_sha256",
	).Where(query.And(query.Equal("source_kind", runtimeDefinitionSourceKind), query.Equal("source_id", s.runtimeID))).Build()
	if err != nil {
		return false, err
	}
	var generation uint64
	var sessionHash string
	var revision int64
	var contentHash string
	if err := tx.QueryRowContext(ctx, selectStatement, selectArgs...).Scan(&generation, &sessionHash, &revision, &contentHash); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return generation == fence.Generation && sessionHash == fence.SessionSHA256 && revision == snapshot.Revision && contentHash == snapshot.ContentSHA256, nil
}

func (s *Store) Reschedule(ctx context.Context, key string, nextRunAt time.Time, _ string) error {
	key = strings.TrimSpace(key)
	if key == "" || nextRunAt.IsZero() {
		return fmt.Errorf("Scheduler definition key and next run time are required")
	}
	update, args, err := query.NewUpdateBuilder(s.dialect, "_scheduler_definition_states").
		Set("next_run_at", formatTime(nextRunAt.UTC())).Set("updated_at", formatTime(time.Now().UTC())).
		Where(query.And(query.Equal("runtime_id", s.runtimeID), query.Equal("definition_key", key))).Build()
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, update, args...)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("Scheduler definition %q is not reconciled", key)
	}
	return nil
}

func (s *Store) Due(ctx context.Context, now time.Time, limit int) ([]modulehost.DueTrigger, error) {
	if limit <= 0 {
		limit = 25
	}
	at := formatTime(now)
	queryValue, args, err := query.NewSelectBuilder(s.dialect, "_scheduler_definition_states").Columns("definition_json", "next_run_at").Where(query.And(query.Equal("runtime_id", s.runtimeID), query.Equal("enabled", true), query.LessThanOrEqual("next_run_at", at))).OrderBy(query.Ascending("next_run_at")).Limit(limit).Build()
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, queryValue, args...)
	if err != nil {
		return nil, err
	}
	states, err := scanDueRows(rows)
	if err != nil {
		return nil, err
	}
	result := make([]modulehost.DueTrigger, 0, limit)
	for _, state := range states {
		remaining := limit - len(result)
		if remaining <= 0 {
			break
		}
		resolution := schedule.ResolveMisfire(state.Definition.Schedule, state.Definition.Policy, state.Cursor, now, remaining)
		if len(resolution.Windows) == 0 {
			if !resolution.CursorAfter.IsZero() || resolution.Disable {
				if _, err := s.fastForwardMisfire(ctx, state.Definition, state.Cursor, resolution.CursorAfter, resolution.Disable, now); err != nil {
					return nil, err
				}
			}
			continue
		}
		for index, window := range resolution.Windows {
			item := modulehost.DueTrigger{Definition: state.Definition, ScheduledFor: window, ExpectedCursor: window}
			if index == len(resolution.Windows)-1 {
				item.NextRunAt = resolution.CursorAfter
			}
			result = append(result, item)
		}
	}
	remaining := limit - len(result)
	if remaining <= 0 {
		return result, nil
	}
	retryQuery, retryArgs, err := query.NewSelectBuilder(s.dialect, "_scheduler_runs").Columns("definition_key", "scheduled_for").Where(query.And(
		query.Equal("runtime_id", s.runtimeID),
		query.Or(
			query.And(query.Equal("status", "retrying"), query.LessThanOrEqual("next_retry_at", at)),
			query.And(query.Equal("status", "leased"), query.LessThanOrEqual("lease_expires_at", at)),
		),
	)).OrderBy(query.Ascending("scheduled_for")).Limit(remaining).Build()
	if err != nil {
		return nil, err
	}
	retries, err := s.db.QueryContext(ctx, retryQuery, retryArgs...)
	if err != nil {
		return nil, err
	}
	defer retries.Close()
	for retries.Next() {
		var key, scheduled string
		if err := retries.Scan(&key, &scheduled); err != nil {
			return nil, err
		}
		definition, found, err := s.definition(ctx, key)
		if err != nil {
			return nil, err
		}
		if found {
			result = append(result, modulehost.DueTrigger{Definition: definition, ScheduledFor: parseTime(scheduled)})
		}
	}
	return result, retries.Err()
}

type dueDefinitionState struct {
	Definition schedulersdk.Definition
	Cursor     time.Time
}

func scanDueRows(rows *sql.Rows) ([]dueDefinitionState, error) {
	defer rows.Close()
	var result []dueDefinitionState
	for rows.Next() {
		var raw, due string
		if err := rows.Scan(&raw, &due); err != nil {
			return nil, err
		}
		var definition schedulersdk.Definition
		if err := json.Unmarshal([]byte(raw), &definition); err != nil {
			return nil, err
		}
		result = append(result, dueDefinitionState{Definition: definition, Cursor: parseTime(due)})
	}
	return result, rows.Err()
}

func (s *Store) fastForwardMisfire(ctx context.Context, definition schedulersdk.Definition, expected, next time.Time, disable bool, now time.Time) (bool, error) {
	if next.IsZero() {
		next = expected
	}
	builder := query.NewUpdateBuilder(s.dialect, "_scheduler_definition_states").
		Set("next_run_at", formatTime(next)).Set("last_run_at", formatTime(expected)).Set("last_run_status", "misfire_skipped").Set("updated_at", formatTime(now)).
		Where(query.And(s.definitionIdentity(definition.Key), query.Equal("enabled", true), query.Equal("next_run_at", formatTime(expected))))
	if disable {
		builder.Set("enabled", false)
	}
	statement, args, err := builder.Build()
	if err != nil {
		return false, err
	}
	result, err := s.db.ExecContext(ctx, statement, args...)
	if err != nil {
		return false, err
	}
	affected, _ := result.RowsAffected()
	return affected == 1, nil
}

func (s *Store) Claim(ctx context.Context, due modulehost.DueTrigger, ttl time.Duration) (schedulersdk.Run, bool, error) {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	now := time.Now().UTC()
	id := runID(s.runtimeID, due.Definition.Key, due.ScheduledFor)
	target, _ := json.Marshal(due.Definition.Target)
	windowKey := due.ScheduledFor.UTC().Format(time.RFC3339)
	insert, args, err := query.NewInsertBuilder(s.dialect, "_scheduler_runs").Columns("runtime_id", "run_id", "definition_key", "definition_revision", "scheduled_for", "window_key", "target_json", "metadata_json", "status", "attempt", "lease_owner", "lease_expires_at", "fencing_token", "created_at", "updated_at").Values(s.runtimeID, id, due.Definition.Key, due.Definition.Revision, formatTime(due.ScheduledFor), windowKey, string(target), nullable(string(due.Metadata)), "leased", 1, s.workerID, formatTime(now.Add(ttl)), int64(1), formatTime(now), formatTime(now)).Build()
	if err != nil {
		return schedulersdk.Run{}, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return schedulersdk.Run{}, false, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, insert, args...); err != nil {
		if !isUnique(err) {
			return schedulersdk.Run{}, false, err
		}
		_ = tx.Rollback()
		run, claimed, takeErr := s.takeExpired(ctx, id, ttl, now)
		if takeErr != nil || claimed {
			return run, claimed, takeErr
		}
		run, getErr := s.get(ctx, id)
		return run, false, getErr
	}
	if !due.ExpectedCursor.IsZero() {
		advanced, err := s.advanceCursorWith(ctx, tx, due.Definition, due.ExpectedCursor, due.NextRunAt, now, "leased")
		if err != nil {
			return schedulersdk.Run{}, false, err
		}
		if !advanced {
			return schedulersdk.Run{}, false, nil
		}
	}
	if err = s.eventWith(ctx, tx, id, "lease_acquired", ""); err != nil {
		return schedulersdk.Run{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return schedulersdk.Run{}, false, err
	}
	run := schedulersdk.Run{Trigger: schedulersdk.Trigger{RunID: id, DefinitionKey: due.Definition.Key, DefinitionRev: due.Definition.Revision, ScheduledFor: due.ScheduledFor.UTC(), WindowKey: windowKey, Target: due.Definition.Target, IdempotencyKey: id, Attempt: 1, Metadata: append(json.RawMessage(nil), due.Metadata...)}, Lease: schedulersdk.Lease{Owner: s.workerID, Token: 1, ExpiresAt: now.Add(ttl)}, Status: "leased", CreatedAt: now, UpdatedAt: now}
	return run, true, nil
}

func (s *Store) takeExpired(ctx context.Context, id string, ttl time.Duration, now time.Time) (schedulersdk.Run, bool, error) {
	predicate := query.And(query.Equal("runtime_id", s.runtimeID), query.Equal("run_id", id), query.Or(query.And(query.Equal("status", "leased"), query.LessThanOrEqual("lease_expires_at", formatTime(now))), query.And(query.Equal("status", "retrying"), query.LessThanOrEqual("next_retry_at", formatTime(now)))))
	update, args, err := query.NewUpdateBuilder(s.dialect, "_scheduler_runs").Set("status", "leased").Set("lease_owner", s.workerID).Set("lease_expires_at", formatTime(now.Add(ttl))).SetExpression("fencing_token", query.Add(query.Column("fencing_token"), query.Value(1))).SetExpression("attempt", query.Add(query.Column("attempt"), query.Value(1))).Set("updated_at", formatTime(now)).Where(predicate).Build()
	if err != nil {
		return schedulersdk.Run{}, false, err
	}
	result, err := s.db.ExecContext(ctx, update, args...)
	if err != nil {
		return schedulersdk.Run{}, false, err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return schedulersdk.Run{}, false, nil
	}
	run, err := s.get(ctx, id)
	return run, err == nil, err
}

func (s *Store) Renew(ctx context.Context, run schedulersdk.Run, ttl time.Duration) (schedulersdk.Run, bool, error) {
	now := time.Now().UTC()
	predicate := s.liveLease(run)
	update, args, err := query.NewUpdateBuilder(s.dialect, "_scheduler_runs").Set("lease_expires_at", formatTime(now.Add(ttl))).Set("updated_at", formatTime(now)).Where(predicate).Build()
	if err != nil {
		return run, false, err
	}
	result, err := s.db.ExecContext(ctx, update, args...)
	if err != nil {
		return run, false, err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return run, false, nil
	}
	run.Lease.ExpiresAt = now.Add(ttl)
	run.UpdatedAt = now
	return run, true, nil
}
func (s *Store) Accept(ctx context.Context, run schedulersdk.Run, receipt schedulersdk.DownstreamReceipt) error {
	raw, _ := json.Marshal(receipt)
	return s.finish(ctx, run, "succeeded", string(raw), "")
}
func (s *Store) Fail(ctx context.Context, run schedulersdk.Run, failure error, retryAt time.Time) error {
	definition, found, err := s.definition(ctx, run.Trigger.DefinitionKey)
	if err != nil {
		return err
	}
	maxAttempts := 3
	if found && definition.Policy.MaxAttempts > 0 {
		maxAttempts = definition.Policy.MaxAttempts
	}
	terminal := run.Trigger.Attempt >= maxAttempts
	status, eventType := "retrying", "retry_scheduled"
	if terminal {
		status, eventType = "dead_letter", "dead_lettered"
	}
	builder := query.NewUpdateBuilder(s.dialect, "_scheduler_runs").Set("status", status).Set("last_error", failure.Error()).Set("lease_owner", nil).Set("lease_expires_at", nil).Set("updated_at", formatTime(time.Now())).Where(s.liveLease(run))
	if terminal {
		builder.Set("next_retry_at", nil)
	} else {
		builder.Set("next_retry_at", formatTime(retryAt))
	}
	update, args, err := builder.Build()
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, update, args...)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return fmt.Errorf("Scheduler lease lost for run %s", run.Trigger.RunID)
	}
	if terminal {
		insert, insertArgs, buildErr := query.NewInsertBuilder(s.dialect, "_scheduler_dead_letters").Columns("runtime_id", "run_id", "definition_key", "reason", "failed_at").Values(s.runtimeID, run.Trigger.RunID, run.Trigger.DefinitionKey, failure.Error(), formatTime(time.Now())).Build()
		if buildErr != nil {
			return buildErr
		}
		if _, err := tx.ExecContext(ctx, insert, insertArgs...); err != nil {
			return err
		}
	}
	if err := s.eventWith(ctx, tx, run.Trigger.RunID, eventType, failure.Error()); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) List(ctx context.Context, limit int) ([]schedulersdk.Run, error) {
	if limit <= 0 {
		limit = 50
	}
	queryValue, args, err := query.NewSelectBuilder(s.dialect, "_scheduler_runs").Columns("run_id").Where(query.Equal("runtime_id", s.runtimeID)).OrderBy(query.Descending("created_at")).Limit(limit).Build()
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, queryValue, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]schedulersdk.Run, 0, len(ids))
	for _, id := range ids {
		run, err := s.get(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	return out, nil
}

func (s *Store) Get(ctx context.Context, id string) (schedulersdk.Run, error) {
	return s.get(ctx, strings.TrimSpace(id))
}

func (s *Store) Retry(ctx context.Context, id, reason string) (schedulersdk.Run, error) {
	now := time.Now().UTC()
	predicate := query.And(query.Equal("runtime_id", s.runtimeID), query.Equal("run_id", strings.TrimSpace(id)), query.In("status", "failed", "dead_letter"))
	update, args, err := query.NewUpdateBuilder(s.dialect, "_scheduler_runs").Set("status", "retrying").Set("next_retry_at", formatTime(now)).Set("lease_owner", nil).Set("lease_expires_at", nil).Set("updated_at", formatTime(now)).Where(predicate).Build()
	if err != nil {
		return schedulersdk.Run{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return schedulersdk.Run{}, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, update, args...)
	if err != nil {
		return schedulersdk.Run{}, err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return schedulersdk.Run{}, fmt.Errorf("Scheduler run %q is not retryable", id)
	}
	resolve, resolveArgs, err := query.NewUpdateBuilder(s.dialect, "_scheduler_dead_letters").Set("resolved_at", formatTime(now)).Where(query.And(query.Equal("runtime_id", s.runtimeID), query.Equal("run_id", strings.TrimSpace(id)), query.Equal("resolved_at", nil))).Build()
	if err != nil {
		return schedulersdk.Run{}, err
	}
	if _, err = tx.ExecContext(ctx, resolve, resolveArgs...); err != nil {
		return schedulersdk.Run{}, err
	}
	if err = s.eventWith(ctx, tx, id, "retry_scheduled", strings.TrimSpace(reason)); err != nil {
		return schedulersdk.Run{}, err
	}
	if err = tx.Commit(); err != nil {
		return schedulersdk.Run{}, err
	}
	return s.get(ctx, id)
}

func (s *Store) Cancel(ctx context.Context, id, reason string) (schedulersdk.Run, error) {
	now := time.Now().UTC()
	predicate := query.And(query.Equal("runtime_id", s.runtimeID), query.Equal("run_id", strings.TrimSpace(id)), query.In("status", "leased", "retrying"))
	update, args, err := query.NewUpdateBuilder(s.dialect, "_scheduler_runs").Set("status", "cancelled").Set("next_retry_at", nil).Set("lease_owner", nil).Set("lease_expires_at", nil).SetExpression("fencing_token", query.Add(query.Column("fencing_token"), query.Value(1))).Set("updated_at", formatTime(now)).Where(predicate).Build()
	if err != nil {
		return schedulersdk.Run{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return schedulersdk.Run{}, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, update, args...)
	if err != nil {
		return schedulersdk.Run{}, err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return schedulersdk.Run{}, fmt.Errorf("Scheduler run %q is not cancellable", id)
	}
	if err = s.eventWith(ctx, tx, id, "cancelled", strings.TrimSpace(reason)); err != nil {
		return schedulersdk.Run{}, err
	}
	if err = tx.Commit(); err != nil {
		return schedulersdk.Run{}, err
	}
	return s.get(ctx, id)
}

func (s *Store) DeadLetter(ctx context.Context, runID string) (schedulersdk.DeadLetter, error) {
	queryValue, args, err := query.NewSelectBuilder(s.dialect, "_scheduler_dead_letters").Columns("definition_key", "reason", "failed_at", "resolved_at").Where(query.And(query.Equal("runtime_id", s.runtimeID), query.Equal("run_id", strings.TrimSpace(runID)))).Build()
	if err != nil {
		return schedulersdk.DeadLetter{}, err
	}
	var definition, reason, failed string
	var resolved sql.NullString
	if err = s.db.QueryRowContext(ctx, queryValue, args...).Scan(&definition, &reason, &failed, &resolved); err != nil {
		return schedulersdk.DeadLetter{}, err
	}
	status := "open"
	if resolved.Valid && strings.TrimSpace(resolved.String) != "" {
		status = "resolved"
	}
	return schedulersdk.DeadLetter{RunID: strings.TrimSpace(runID), DefinitionKey: definition, Status: status, Reason: reason, FailedAt: parseTime(failed), ResolvedAt: parseTime(resolved.String)}, nil
}

func (s *Store) DeadLetters(ctx context.Context, limit int) ([]schedulersdk.DeadLetter, error) {
	if limit <= 0 {
		limit = 50
	}
	queryValue, args, err := query.NewSelectBuilder(s.dialect, "_scheduler_dead_letters").
		Columns("run_id", "definition_key", "reason", "failed_at", "resolved_at").
		Where(query.Equal("runtime_id", s.runtimeID)).
		OrderBy(query.Descending("failed_at")).Limit(limit).Build()
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, queryValue, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]schedulersdk.DeadLetter, 0)
	for rows.Next() {
		var runID, definition, reason, failed string
		var resolved sql.NullString
		if err := rows.Scan(&runID, &definition, &reason, &failed, &resolved); err != nil {
			return nil, err
		}
		status := "open"
		if resolved.Valid && strings.TrimSpace(resolved.String) != "" {
			status = "resolved"
		}
		items = append(items, schedulersdk.DeadLetter{
			RunID: runID, DefinitionKey: definition, Status: status, Reason: reason,
			FailedAt: parseTime(failed), ResolvedAt: parseTime(resolved.String),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Store) ResolveDeadLetter(ctx context.Context, runID, reason string) (schedulersdk.DeadLetter, error) {
	now := time.Now().UTC()
	update, args, err := query.NewUpdateBuilder(s.dialect, "_scheduler_dead_letters").Set("resolved_at", formatTime(now)).Where(query.And(query.Equal("runtime_id", s.runtimeID), query.Equal("run_id", strings.TrimSpace(runID)), query.Equal("resolved_at", nil))).Build()
	if err != nil {
		return schedulersdk.DeadLetter{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return schedulersdk.DeadLetter{}, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, update, args...)
	if err != nil {
		return schedulersdk.DeadLetter{}, err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return schedulersdk.DeadLetter{}, fmt.Errorf("Scheduler dead letter %q is not open", runID)
	}
	if err = s.eventWith(ctx, tx, runID, "dead_letter_resolved", strings.TrimSpace(reason)); err != nil {
		return schedulersdk.DeadLetter{}, err
	}
	if err = tx.Commit(); err != nil {
		return schedulersdk.DeadLetter{}, err
	}
	return s.DeadLetter(ctx, runID)
}

func (s *Store) RequeueDeadLetter(ctx context.Context, runID, reason string) (schedulersdk.Run, error) {
	if _, err := s.DeadLetter(ctx, runID); err != nil {
		return schedulersdk.Run{}, err
	}
	return s.Retry(ctx, runID, reason)
}

func (s *Store) finish(ctx context.Context, run schedulersdk.Run, status, receipt, lastError string) error {
	update, args, err := query.NewUpdateBuilder(s.dialect, "_scheduler_runs").Set("status", status).Set("receipt_json", nullable(receipt)).Set("last_error", nullable(lastError)).Set("lease_owner", nil).Set("lease_expires_at", nil).Set("updated_at", formatTime(time.Now())).Where(s.liveLease(run)).Build()
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, update, args...)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return fmt.Errorf("Scheduler lease lost for run %s", run.Trigger.RunID)
	}
	if err := s.eventWith(ctx, tx, run.Trigger.RunID, "state_changed", status); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) get(ctx context.Context, id string) (schedulersdk.Run, error) {
	queryValue, args, err := query.NewSelectBuilder(s.dialect, "_scheduler_runs").Columns("definition_key", "definition_revision", "scheduled_for", "window_key", "target_json", "metadata_json", "status", "attempt", "lease_owner", "lease_expires_at", "fencing_token", "receipt_json", "last_error", "created_at", "updated_at").Where(query.And(query.Equal("runtime_id", s.runtimeID), query.Equal("run_id", id))).Build()
	if err != nil {
		return schedulersdk.Run{}, err
	}
	var def, rev, scheduled, window, target, status, created, updated string
	var metadata, owner, expires, receipt, last sql.NullString
	var attempt int
	var token int64
	if err = s.db.QueryRowContext(ctx, queryValue, args...).Scan(&def, &rev, &scheduled, &window, &target, &metadata, &status, &attempt, &owner, &expires, &token, &receipt, &last, &created, &updated); err != nil {
		return schedulersdk.Run{}, err
	}
	var targetRef schedulersdk.TargetRef
	_ = json.Unmarshal([]byte(target), &targetRef)
	var rc schedulersdk.DownstreamReceipt
	_ = json.Unmarshal([]byte(receipt.String), &rc)
	return schedulersdk.Run{Trigger: schedulersdk.Trigger{RunID: id, DefinitionKey: def, DefinitionRev: rev, ScheduledFor: parseTime(scheduled), WindowKey: window, Target: targetRef, IdempotencyKey: id, Attempt: attempt, Metadata: json.RawMessage(metadata.String)}, Lease: schedulersdk.Lease{Owner: owner.String, Token: token, ExpiresAt: parseTime(expires.String)}, Status: status, DownstreamReceipt: rc, LastError: last.String, CreatedAt: parseTime(created), UpdatedAt: parseTime(updated)}, nil
}

func (s *Store) definition(ctx context.Context, key string) (schedulersdk.Definition, bool, error) {
	queryValue, args, err := query.NewSelectBuilder(s.dialect, "_scheduler_definition_states").Columns("definition_json").Where(s.definitionIdentity(key)).Build()
	if err != nil {
		return schedulersdk.Definition{}, false, err
	}
	var raw string
	err = s.db.QueryRowContext(ctx, queryValue, args...).Scan(&raw)
	if err == sql.ErrNoRows {
		return schedulersdk.Definition{}, false, nil
	}
	if err != nil {
		return schedulersdk.Definition{}, false, err
	}
	var value schedulersdk.Definition
	err = json.Unmarshal([]byte(raw), &value)
	return value, err == nil, err
}
func (s *Store) advanceCursor(ctx context.Context, d schedulersdk.Definition, scheduled, next, now time.Time, status string) (bool, error) {
	return s.advanceCursorWith(ctx, s.db, d, scheduled, next, now, status)
}
func (s *Store) advanceCursorWith(ctx context.Context, executor sqlExecutor, d schedulersdk.Definition, scheduled, next, now time.Time, status string) (bool, error) {
	disable := schedule.Type(schedule.Data(d.Schedule)) == "once"
	if next.IsZero() && !disable {
		next = schedule.NextSchedule(d.Schedule, scheduled)
	}
	if next.IsZero() {
		next = scheduled
	}
	builder := query.NewUpdateBuilder(s.dialect, "_scheduler_definition_states").Set("next_run_at", formatTime(next)).Set("last_run_at", formatTime(scheduled)).Set("last_run_status", status).Set("updated_at", formatTime(now)).Where(query.And(s.definitionIdentity(d.Key), query.Equal("enabled", true), query.Equal("next_run_at", formatTime(scheduled))))
	if disable {
		builder.Set("enabled", false)
	}
	update, args, err := builder.Build()
	if err != nil {
		return false, err
	}
	result, err := executor.ExecContext(ctx, update, args...)
	if err != nil {
		return false, err
	}
	affected, _ := result.RowsAffected()
	return affected == 1, nil
}
func (s *Store) event(ctx context.Context, runID, kind, message string) error {
	return s.eventWith(ctx, s.db, runID, kind, message)
}
func (s *Store) eventWith(ctx context.Context, executor sqlExecutor, runID, kind, message string) error {
	id := runID + ":" + kind + ":" + fmt.Sprint(time.Now().UnixNano())
	insert, args, err := query.NewInsertBuilder(s.dialect, "_scheduler_run_events").Columns("runtime_id", "event_id", "run_id", "event_type", "message", "created_at").Values(s.runtimeID, id, runID, kind, nullable(message), formatTime(time.Now())).Build()
	if err != nil {
		return err
	}
	_, err = executor.ExecContext(ctx, insert, args...)
	return err
}
func (s *Store) definitionIdentity(key string) query.Predicate {
	return query.And(query.Equal("runtime_id", s.runtimeID), query.Equal("definition_key", strings.TrimSpace(key)))
}
func (s *Store) definitionSourceIdentity(key, sourceKind string) query.Predicate {
	return query.And(s.definitionIdentity(key), query.Equal("source_kind", sourceKind))
}
func (s *Store) liveLease(run schedulersdk.Run) query.Predicate {
	return query.And(query.Equal("runtime_id", s.runtimeID), query.Equal("run_id", run.Trigger.RunID), query.Equal("lease_owner", run.Lease.Owner), query.Equal("fencing_token", run.Lease.Token), query.Equal("status", "leased"))
}
func nullable(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}
func formatTime(v time.Time) string {
	if v.IsZero() {
		return ""
	}
	return v.UTC().Format(time.RFC3339Nano)
}
func parseTime(v string) time.Time { t, _ := time.Parse(time.RFC3339Nano, v); return t }
func runID(runtimeID, key string, scheduled time.Time) string {
	sum := sha256.Sum256([]byte(runtimeID + "\x00" + key + "\x00" + formatTime(scheduled)))
	return "run_" + hex.EncodeToString(sum[:16])
}
func isUnique(err error) bool {
	if err == nil {
		return false
	}
	v := strings.ToLower(err.Error())
	return strings.Contains(v, "unique") || strings.Contains(v, "duplicate") || strings.Contains(v, "constraint")
}

var _ modulehost.RunStore = (*Store)(nil)
