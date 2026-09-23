package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	sharedoperation "github.com/domainry/domainry-foundation/operation"
	"github.com/domainry/domainry-foundation/requestcontext"
	schedulermodel "github.com/domainry/domainry-scheduler/internal/domain/scheduler/model"
)

const (
	schedulerOperationPurpose = "scheduler_management"
	schedulerOperationOwner   = "scheduler"
	schedulerOperationKind    = "management_command"
)

type CommandReceiptStore struct {
	operations sharedoperation.Store
	runtimeID  string
}

type commandOperationResult struct {
	HTTPStatus   int    `json:"http_status"`
	ResponseJSON []byte `json:"response_json"`
}

func NewCommandReceiptStore(operations sharedoperation.Store, runtimeID string) (*CommandReceiptStore, error) {
	if operations == nil {
		return nil, fmt.Errorf("Scheduler shared Operation store is required")
	}
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return nil, fmt.Errorf("Scheduler command receipt runtime identity is required")
	}
	return &CommandReceiptStore{operations: operations, runtimeID: runtimeID}, nil
}

func (s *CommandReceiptStore) ClaimCommand(ctx context.Context, receipt schedulermodel.CommandReceipt) (schedulermodel.CommandReceipt, bool, error) {
	receipt.IdempotencyKey = strings.TrimSpace(receipt.IdempotencyKey)
	receipt.ActionKey = strings.TrimSpace(receipt.ActionKey)
	receipt.ResourceKey = strings.TrimSpace(receipt.ResourceKey)
	receipt.RequestHash = strings.TrimSpace(receipt.RequestHash)
	if receipt.IdempotencyKey == "" || receipt.ActionKey == "" || receipt.ResourceKey == "" || receipt.RequestHash == "" {
		return schedulermodel.CommandReceipt{}, false, fmt.Errorf("Scheduler command receipt identity is incomplete")
	}
	now := time.Now().UTC()
	actor := strings.TrimSpace(requestcontext.ActorID(ctx))
	if actor == "" {
		actor = "scheduler:" + s.runtimeID
	}
	stored, claimed, err := s.operations.Claim(ctx, sharedoperation.Command{
		ID: schedulerOperationID(s.runtimeID, receipt.IdempotencyKey),
		Scope: sharedoperation.Scope{
			SystemPurpose: schedulerOperationPurpose,
			ResourceType:  "scheduler_command",
			ResourceID:    s.runtimeID + ":" + receipt.ResourceKey,
		},
		Owner: schedulerOperationOwner, Kind: schedulerOperationKind, ActionKey: receipt.ActionKey,
		IdempotencyKey: schedulerOperationKey(s.runtimeID, receipt.IdempotencyKey), RequestFingerprint: receipt.RequestHash,
		RequestedBy: actor, Reason: "Scheduler management command " + receipt.ActionKey,
		Reference: s.runtimeID, StatusURL: receipt.ResourceKey, CreatedAt: now,
	})
	if err != nil {
		return schedulermodel.CommandReceipt{}, false, err
	}
	return schedulerCommandReceipt(stored, s.runtimeID), claimed, nil
}

func (s *CommandReceiptStore) CompleteCommand(ctx context.Context, idempotencyKey, requestHash string, httpStatus int, responseJSON []byte) error {
	result, err := json.Marshal(commandOperationResult{HTTPStatus: httpStatus, ResponseJSON: append([]byte(nil), responseJSON...)})
	if err != nil {
		return err
	}
	return s.operations.Complete(ctx, sharedoperation.Completion{
		ID:    schedulerOperationID(s.runtimeID, idempotencyKey),
		Scope: sharedoperation.Scope{SystemPurpose: schedulerOperationPurpose},
		Owner: schedulerOperationOwner, Kind: schedulerOperationKind,
		IdempotencyKey: schedulerOperationKey(s.runtimeID, idempotencyKey), RequestFingerprint: strings.TrimSpace(requestHash),
		Result: result, CompletedAt: time.Now().UTC(),
	})
}

func schedulerCommandReceipt(stored sharedoperation.Receipt, runtimeID string) schedulermodel.CommandReceipt {
	receipt := schedulermodel.CommandReceipt{
		IdempotencyKey: strings.TrimPrefix(stored.Command.IdempotencyKey, runtimeID+":"),
		ActionKey:      stored.Command.ActionKey, ResourceKey: stored.Command.StatusURL,
		RequestHash: stored.Command.RequestFingerprint,
	}
	switch stored.Status {
	case sharedoperation.StatusStarted:
		receipt.Status = schedulermodel.CommandReceiptExecuting
	case sharedoperation.StatusSucceeded:
		receipt.Status = schedulermodel.CommandReceiptCompleted
		var result commandOperationResult
		if json.Unmarshal(stored.Result, &result) == nil {
			receipt.HTTPStatus = result.HTTPStatus
			receipt.ResponseJSON = append([]byte(nil), result.ResponseJSON...)
		}
	}
	return receipt
}

func schedulerOperationKey(runtimeID, idempotencyKey string) string {
	return strings.TrimSpace(runtimeID) + ":" + strings.TrimSpace(idempotencyKey)
}

func schedulerOperationID(runtimeID, idempotencyKey string) string {
	digest := sha256.Sum256([]byte(schedulerOperationKey(runtimeID, idempotencyKey)))
	return "scheduler_operation:" + hex.EncodeToString(digest[:16])
}

var _ schedulermodel.CommandReceiptStore = (*CommandReceiptStore)(nil)
