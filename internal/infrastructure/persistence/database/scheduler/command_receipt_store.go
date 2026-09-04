package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/domainry/domainry-orm/query"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulermodel "github.com/domainry/domainry-scheduler/internal/domain/scheduler/model"
)

type CommandReceiptStore struct {
	db        modulehost.Database
	dialect   modulehost.Dialect
	runtimeID string
}

func NewCommandReceiptStore(db modulehost.Database, dialect modulehost.Dialect, runtimeID string) (*CommandReceiptStore, error) {
	if db == nil {
		return nil, fmt.Errorf("Scheduler command receipt database is required")
	}
	if dialect == nil {
		return nil, fmt.Errorf("Scheduler command receipt dialect is required")
	}
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return nil, fmt.Errorf("Scheduler command receipt runtime identity is required")
	}
	return &CommandReceiptStore{db: db, dialect: dialect, runtimeID: runtimeID}, nil
}

func (s *CommandReceiptStore) ClaimCommand(ctx context.Context, receipt schedulermodel.CommandReceipt) (schedulermodel.CommandReceipt, bool, error) {
	receipt.IdempotencyKey = strings.TrimSpace(receipt.IdempotencyKey)
	receipt.ActionKey = strings.TrimSpace(receipt.ActionKey)
	receipt.ResourceKey = strings.TrimSpace(receipt.ResourceKey)
	receipt.RequestHash = strings.TrimSpace(receipt.RequestHash)
	if receipt.IdempotencyKey == "" || receipt.ActionKey == "" || receipt.ResourceKey == "" || receipt.RequestHash == "" {
		return schedulermodel.CommandReceipt{}, false, fmt.Errorf("Scheduler command receipt identity is incomplete")
	}
	now := formatTime(time.Now().UTC())
	statement, args, err := query.NewInsertBuilder(s.dialect, "_scheduler_command_receipts").
		Columns("runtime_id", "idempotency_key", "action_key", "resource_key", "request_hash", "status", "http_status", "response_json", "created_at", "updated_at").
		Values(s.runtimeID, receipt.IdempotencyKey, receipt.ActionKey, receipt.ResourceKey, receipt.RequestHash, schedulermodel.CommandReceiptExecuting, int64(0), nil, now, now).
		Build()
	if err != nil {
		return schedulermodel.CommandReceipt{}, false, err
	}
	if _, err = s.db.ExecContext(ctx, statement, args...); err == nil {
		receipt.Status = schedulermodel.CommandReceiptExecuting
		return receipt, true, nil
	} else if !isUnique(err) {
		return schedulermodel.CommandReceipt{}, false, err
	}
	existing, err := s.commandReceipt(ctx, receipt.IdempotencyKey)
	return existing, false, err
}

func (s *CommandReceiptStore) CompleteCommand(ctx context.Context, idempotencyKey, requestHash string, httpStatus int, responseJSON []byte) error {
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_scheduler_command_receipts").
		Set("status", schedulermodel.CommandReceiptCompleted).
		Set("http_status", int64(httpStatus)).
		Set("response_json", string(responseJSON)).
		Set("updated_at", formatTime(time.Now().UTC())).
		Where(query.And(
			query.Equal("runtime_id", s.runtimeID),
			query.Equal("idempotency_key", strings.TrimSpace(idempotencyKey)),
			query.Equal("request_hash", strings.TrimSpace(requestHash)),
			query.Equal("status", schedulermodel.CommandReceiptExecuting),
		)).Build()
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, statement, args...)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("Scheduler command receipt %q was not executing", idempotencyKey)
	}
	return nil
}

func (s *CommandReceiptStore) commandReceipt(ctx context.Context, idempotencyKey string) (schedulermodel.CommandReceipt, error) {
	statement, args, err := query.NewSelectBuilder(s.dialect, "_scheduler_command_receipts").
		Columns("action_key", "resource_key", "request_hash", "status", "http_status", "response_json").
		Where(query.And(query.Equal("runtime_id", s.runtimeID), query.Equal("idempotency_key", strings.TrimSpace(idempotencyKey)))).
		Build()
	if err != nil {
		return schedulermodel.CommandReceipt{}, err
	}
	var receipt schedulermodel.CommandReceipt
	var response sql.NullString
	var status int64
	if err := s.db.QueryRowContext(ctx, statement, args...).Scan(&receipt.ActionKey, &receipt.ResourceKey, &receipt.RequestHash, &receipt.Status, &status, &response); err != nil {
		return schedulermodel.CommandReceipt{}, err
	}
	receipt.IdempotencyKey = strings.TrimSpace(idempotencyKey)
	receipt.HTTPStatus = int(status)
	if response.Valid {
		receipt.ResponseJSON = []byte(response.String)
	}
	return receipt, nil
}

var _ schedulermodel.CommandReceiptStore = (*CommandReceiptStore)(nil)
