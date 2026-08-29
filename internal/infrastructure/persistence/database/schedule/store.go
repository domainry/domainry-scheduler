package schedule

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	ormbuilder "github.com/domainry/domainry-orm/builder"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	"github.com/domainry/domainry-scheduler-sdk/schedule"
)

type Store struct {
	db                  modulehost.Database
	dialect             modulehost.Dialect
	runtimeID, workerID string
}

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
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	now := formatTime(time.Now())
	predicate := s.definitionIdentity(d.Key)
	// A revision change is the only definition reconciliation allowed to move
	// an existing durable cursor. Same-revision restarts preserve next_run_at.
	changed, changedArgs, err := ormbuilder.NewUpdateBuilder(s.dialect, "scheduler_schedule_state").
		Set("revision", d.Revision).Set("enabled", true).Set("definition_json", string(raw)).Set("next_run_at", formatTime(next)).Set("updated_at", now).
		Where(ormbuilder.And(predicate, ormbuilder.NotEqual("revision", d.Revision))).Build()
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, changed, changedArgs...)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected > 0 {
		return nil
	}
	stable, stableArgs, err := ormbuilder.NewUpdateBuilder(s.dialect, "scheduler_schedule_state").
		Set("enabled", true).Set("definition_json", string(raw)).Set("updated_at", now).Where(predicate).Build()
	if err != nil {
		return err
	}
	result, err = s.db.ExecContext(ctx, stable, stableArgs...)
	if err != nil {
		return err
	}
	affected, _ = result.RowsAffected()
	if affected > 0 {
		return nil
	}
	insert, insertArgs, err := ormbuilder.NewInsertBuilder(s.dialect, "scheduler_schedule_state").Columns("runtime_id", "definition_key", "revision", "enabled", "definition_json", "next_run_at", "snapshot_revision", "updated_at").Values(s.runtimeID, d.Key, d.Revision, true, string(raw), formatTime(next), int64(0), now).Build()
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, insert, insertArgs...)
	if err == nil {
		return nil
	}
	if !isUnique(err) {
		return err
	}
	// Either a peer inserted between update and insert, or the database reports
	// only changed (not matched) rows. In both cases the identity now exists.
	return nil
}

func (s *Store) DisableMissing(ctx context.Context, active []string, revision int64) error {
	update, args, err := ormbuilder.NewUpdateBuilder(s.dialect, "scheduler_schedule_state").Set("enabled", false).Set("snapshot_revision", revision).Where(ormbuilder.Equal("runtime_id", s.runtimeID)).Build()
	if err != nil {
		return err
	}
	if _, err = s.db.ExecContext(ctx, update, args...); err != nil {
		return err
	}
	if len(active) == 0 {
		return nil
	}
	values := make([]any, len(active))
	for i, key := range active {
		values[i] = key
	}
	update, args, err = ormbuilder.NewUpdateBuilder(s.dialect, "scheduler_schedule_state").Set("enabled", true).Set("snapshot_revision", revision).Where(ormbuilder.And(ormbuilder.Equal("runtime_id", s.runtimeID), ormbuilder.In("definition_key", values...))).Build()
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, update, args...)
	return err
}

func (s *Store) Reschedule(ctx context.Context, key string, nextRunAt time.Time, _ string) error {
	key = strings.TrimSpace(key)
	if key == "" || nextRunAt.IsZero() {
		return fmt.Errorf("Scheduler definition key and next run time are required")
	}
	update, args, err := ormbuilder.NewUpdateBuilder(s.dialect, "scheduler_schedule_state").
		Set("next_run_at", formatTime(nextRunAt.UTC())).Set("updated_at", formatTime(time.Now().UTC())).
		Where(ormbuilder.And(ormbuilder.Equal("runtime_id", s.runtimeID), ormbuilder.Equal("definition_key", key))).Build()
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
	query, args, err := ormbuilder.NewSelectBuilder(s.dialect, "scheduler_schedule_state").Columns("definition_json", "next_run_at").Where(ormbuilder.And(ormbuilder.Equal("runtime_id", s.runtimeID), ormbuilder.Equal("enabled", true), ormbuilder.LessThanOrEqual("next_run_at", at))).OrderBy(ormbuilder.Ascending("next_run_at")).Limit(limit).Build()
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	result, err := scanDueRows(rows)
	if err != nil {
		return nil, err
	}
	remaining := limit - len(result)
	if remaining <= 0 {
		return result, nil
	}
	retryQuery, retryArgs, err := ormbuilder.NewSelectBuilder(s.dialect, "scheduler_runs").Columns("definition_key", "scheduled_for").Where(ormbuilder.And(ormbuilder.Equal("runtime_id", s.runtimeID), ormbuilder.Equal("status", "retrying"), ormbuilder.LessThanOrEqual("next_retry_at", at))).OrderBy(ormbuilder.Ascending("next_retry_at")).Limit(remaining).Build()
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

func scanDueRows(rows *sql.Rows) ([]modulehost.DueTrigger, error) {
	defer rows.Close()
	var result []modulehost.DueTrigger
	for rows.Next() {
		var raw, due string
		if err := rows.Scan(&raw, &due); err != nil {
			return nil, err
		}
		var definition schedulersdk.Definition
		if err := json.Unmarshal([]byte(raw), &definition); err != nil {
			return nil, err
		}
		result = append(result, modulehost.DueTrigger{Definition: definition, ScheduledFor: parseTime(due)})
	}
	return result, rows.Err()
}

func (s *Store) Claim(ctx context.Context, due modulehost.DueTrigger, ttl time.Duration) (schedulersdk.Run, bool, error) {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	now := time.Now().UTC()
	id := runID(s.runtimeID, due.Definition.Key, due.ScheduledFor)
	target, _ := json.Marshal(due.Definition.Target)
	insert, args, err := ormbuilder.NewInsertBuilder(s.dialect, "scheduler_runs").Columns("runtime_id", "run_id", "definition_key", "definition_revision", "scheduled_for", "window_key", "target_json", "status", "attempt", "lease_owner", "lease_expires_at", "fencing_token", "created_at", "updated_at").Values(s.runtimeID, id, due.Definition.Key, due.Definition.Revision, formatTime(due.ScheduledFor), due.ScheduledFor.UTC().Format(time.RFC3339), string(target), "leased", 1, s.workerID, formatTime(now.Add(ttl)), int64(1), formatTime(now), formatTime(now)).Build()
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
		return s.takeExpired(ctx, id, ttl, now)
	}
	if err = s.advanceCursorWith(ctx, tx, due.Definition, due.ScheduledFor, now, "leased"); err != nil {
		return schedulersdk.Run{}, false, err
	}
	if err = s.eventWith(ctx, tx, id, "lease_acquired", ""); err != nil {
		return schedulersdk.Run{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return schedulersdk.Run{}, false, err
	}
	run := schedulersdk.Run{Trigger: schedulersdk.Trigger{RunID: id, DefinitionKey: due.Definition.Key, DefinitionRev: due.Definition.Revision, ScheduledFor: due.ScheduledFor, WindowKey: due.ScheduledFor.UTC().Format(time.RFC3339), Target: due.Definition.Target, IdempotencyKey: id, Attempt: 1}, Lease: schedulersdk.Lease{Owner: s.workerID, Token: 1, ExpiresAt: now.Add(ttl)}, Status: "leased", CreatedAt: now, UpdatedAt: now}
	return run, true, nil
}

func (s *Store) takeExpired(ctx context.Context, id string, ttl time.Duration, now time.Time) (schedulersdk.Run, bool, error) {
	predicate := ormbuilder.And(ormbuilder.Equal("runtime_id", s.runtimeID), ormbuilder.Equal("run_id", id), ormbuilder.Or(ormbuilder.And(ormbuilder.Equal("status", "leased"), ormbuilder.LessThanOrEqual("lease_expires_at", formatTime(now))), ormbuilder.And(ormbuilder.Equal("status", "retrying"), ormbuilder.LessThanOrEqual("next_retry_at", formatTime(now)))))
	update, args, err := ormbuilder.NewUpdateBuilder(s.dialect, "scheduler_runs").Set("status", "leased").Set("lease_owner", s.workerID).Set("lease_expires_at", formatTime(now.Add(ttl))).SetExpression("fencing_token", ormbuilder.Add(ormbuilder.Column("fencing_token"), ormbuilder.Value(1))).SetExpression("attempt", ormbuilder.Add(ormbuilder.Column("attempt"), ormbuilder.Value(1))).Set("updated_at", formatTime(now)).Where(predicate).Build()
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
	update, args, err := ormbuilder.NewUpdateBuilder(s.dialect, "scheduler_runs").Set("lease_expires_at", formatTime(now.Add(ttl))).Set("updated_at", formatTime(now)).Where(predicate).Build()
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
	builder := ormbuilder.NewUpdateBuilder(s.dialect, "scheduler_runs").Set("status", status).Set("last_error", failure.Error()).Set("lease_owner", nil).Set("lease_expires_at", nil).Set("updated_at", formatTime(time.Now())).Where(s.liveLease(run))
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
		insert, insertArgs, buildErr := ormbuilder.NewInsertBuilder(s.dialect, "scheduler_dead_letters").Columns("runtime_id", "run_id", "definition_key", "reason", "failed_at").Values(s.runtimeID, run.Trigger.RunID, run.Trigger.DefinitionKey, failure.Error(), formatTime(time.Now())).Build()
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
	query, args, err := ormbuilder.NewSelectBuilder(s.dialect, "scheduler_runs").Columns("run_id").Where(ormbuilder.Equal("runtime_id", s.runtimeID)).OrderBy(ormbuilder.Descending("created_at")).Limit(limit).Build()
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
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
	predicate := ormbuilder.And(ormbuilder.Equal("runtime_id", s.runtimeID), ormbuilder.Equal("run_id", strings.TrimSpace(id)), ormbuilder.In("status", "failed", "dead_letter"))
	update, args, err := ormbuilder.NewUpdateBuilder(s.dialect, "scheduler_runs").Set("status", "retrying").Set("next_retry_at", formatTime(now)).Set("lease_owner", nil).Set("lease_expires_at", nil).Set("updated_at", formatTime(now)).Where(predicate).Build()
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
	resolve, resolveArgs, err := ormbuilder.NewUpdateBuilder(s.dialect, "scheduler_dead_letters").Set("resolved_at", formatTime(now)).Where(ormbuilder.And(ormbuilder.Equal("runtime_id", s.runtimeID), ormbuilder.Equal("run_id", strings.TrimSpace(id)), ormbuilder.Equal("resolved_at", nil))).Build()
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
	predicate := ormbuilder.And(ormbuilder.Equal("runtime_id", s.runtimeID), ormbuilder.Equal("run_id", strings.TrimSpace(id)), ormbuilder.In("status", "leased", "retrying"))
	update, args, err := ormbuilder.NewUpdateBuilder(s.dialect, "scheduler_runs").Set("status", "cancelled").Set("next_retry_at", nil).Set("lease_owner", nil).Set("lease_expires_at", nil).SetExpression("fencing_token", ormbuilder.Add(ormbuilder.Column("fencing_token"), ormbuilder.Value(1))).Set("updated_at", formatTime(now)).Where(predicate).Build()
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
	query, args, err := ormbuilder.NewSelectBuilder(s.dialect, "scheduler_dead_letters").Columns("definition_key", "reason", "failed_at", "resolved_at").Where(ormbuilder.And(ormbuilder.Equal("runtime_id", s.runtimeID), ormbuilder.Equal("run_id", strings.TrimSpace(runID)))).Build()
	if err != nil {
		return schedulersdk.DeadLetter{}, err
	}
	var definition, reason, failed string
	var resolved sql.NullString
	if err = s.db.QueryRowContext(ctx, query, args...).Scan(&definition, &reason, &failed, &resolved); err != nil {
		return schedulersdk.DeadLetter{}, err
	}
	status := "open"
	if resolved.Valid && strings.TrimSpace(resolved.String) != "" {
		status = "resolved"
	}
	return schedulersdk.DeadLetter{RunID: strings.TrimSpace(runID), DefinitionKey: definition, Status: status, Reason: reason, FailedAt: parseTime(failed), ResolvedAt: parseTime(resolved.String)}, nil
}

func (s *Store) ResolveDeadLetter(ctx context.Context, runID, reason string) (schedulersdk.DeadLetter, error) {
	now := time.Now().UTC()
	update, args, err := ormbuilder.NewUpdateBuilder(s.dialect, "scheduler_dead_letters").Set("resolved_at", formatTime(now)).Where(ormbuilder.And(ormbuilder.Equal("runtime_id", s.runtimeID), ormbuilder.Equal("run_id", strings.TrimSpace(runID)), ormbuilder.Equal("resolved_at", nil))).Build()
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
	update, args, err := ormbuilder.NewUpdateBuilder(s.dialect, "scheduler_runs").Set("status", status).Set("receipt_json", nullable(receipt)).Set("last_error", nullable(lastError)).Set("lease_owner", nil).Set("lease_expires_at", nil).Set("updated_at", formatTime(time.Now())).Where(s.liveLease(run)).Build()
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
	query, args, err := ormbuilder.NewSelectBuilder(s.dialect, "scheduler_runs").Columns("definition_key", "definition_revision", "scheduled_for", "window_key", "target_json", "metadata_json", "status", "attempt", "lease_owner", "lease_expires_at", "fencing_token", "receipt_json", "last_error", "created_at", "updated_at").Where(ormbuilder.And(ormbuilder.Equal("runtime_id", s.runtimeID), ormbuilder.Equal("run_id", id))).Build()
	if err != nil {
		return schedulersdk.Run{}, err
	}
	var def, rev, scheduled, window, target, status, created, updated string
	var metadata, owner, expires, receipt, last sql.NullString
	var attempt int
	var token int64
	if err = s.db.QueryRowContext(ctx, query, args...).Scan(&def, &rev, &scheduled, &window, &target, &metadata, &status, &attempt, &owner, &expires, &token, &receipt, &last, &created, &updated); err != nil {
		return schedulersdk.Run{}, err
	}
	var targetRef schedulersdk.TargetRef
	_ = json.Unmarshal([]byte(target), &targetRef)
	var rc schedulersdk.DownstreamReceipt
	_ = json.Unmarshal([]byte(receipt.String), &rc)
	return schedulersdk.Run{Trigger: schedulersdk.Trigger{RunID: id, DefinitionKey: def, DefinitionRev: rev, ScheduledFor: parseTime(scheduled), WindowKey: window, Target: targetRef, IdempotencyKey: id, Attempt: attempt, Metadata: json.RawMessage(metadata.String)}, Lease: schedulersdk.Lease{Owner: owner.String, Token: token, ExpiresAt: parseTime(expires.String)}, Status: status, DownstreamReceipt: rc, LastError: last.String, CreatedAt: parseTime(created), UpdatedAt: parseTime(updated)}, nil
}

func (s *Store) definition(ctx context.Context, key string) (schedulersdk.Definition, bool, error) {
	query, args, err := ormbuilder.NewSelectBuilder(s.dialect, "scheduler_schedule_state").Columns("definition_json").Where(s.definitionIdentity(key)).Build()
	if err != nil {
		return schedulersdk.Definition{}, false, err
	}
	var raw string
	err = s.db.QueryRowContext(ctx, query, args...).Scan(&raw)
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
func (s *Store) advanceCursor(ctx context.Context, d schedulersdk.Definition, scheduled, now time.Time, status string) error {
	return s.advanceCursorWith(ctx, s.db, d, scheduled, now, status)
}
func (s *Store) advanceCursorWith(ctx context.Context, executor sqlExecutor, d schedulersdk.Definition, scheduled, now time.Time, status string) error {
	next := schedule.NextSchedule(d.Schedule, scheduled)
	update, args, err := ormbuilder.NewUpdateBuilder(s.dialect, "scheduler_schedule_state").Set("next_run_at", formatTime(next)).Set("last_run_at", formatTime(scheduled)).Set("last_run_status", status).Set("updated_at", formatTime(now)).Where(s.definitionIdentity(d.Key)).Build()
	if err != nil {
		return err
	}
	_, err = executor.ExecContext(ctx, update, args...)
	return err
}
func (s *Store) event(ctx context.Context, runID, kind, message string) error {
	return s.eventWith(ctx, s.db, runID, kind, message)
}
func (s *Store) eventWith(ctx context.Context, executor sqlExecutor, runID, kind, message string) error {
	id := runID + ":" + kind + ":" + fmt.Sprint(time.Now().UnixNano())
	insert, args, err := ormbuilder.NewInsertBuilder(s.dialect, "scheduler_run_events").Columns("runtime_id", "event_id", "run_id", "event_type", "message", "created_at").Values(s.runtimeID, id, runID, kind, nullable(message), formatTime(time.Now())).Build()
	if err != nil {
		return err
	}
	_, err = executor.ExecContext(ctx, insert, args...)
	return err
}
func (s *Store) definitionIdentity(key string) ormbuilder.Predicate {
	return ormbuilder.And(ormbuilder.Equal("runtime_id", s.runtimeID), ormbuilder.Equal("definition_key", strings.TrimSpace(key)))
}
func (s *Store) liveLease(run schedulersdk.Run) ormbuilder.Predicate {
	return ormbuilder.And(ormbuilder.Equal("runtime_id", s.runtimeID), ormbuilder.Equal("run_id", run.Trigger.RunID), ormbuilder.Equal("lease_owner", run.Lease.Owner), ormbuilder.Equal("fencing_token", run.Lease.Token), ormbuilder.Equal("status", "leased"))
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
