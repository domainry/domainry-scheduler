package model

import "context"

const (
	CommandReceiptExecuting = "executing"
	CommandReceiptCompleted = "completed"
)

// CommandReceipt is the Scheduler-owned durable idempotency evidence for one
// externally invoked management command. A receipt is claimed before the
// command runs and completed before its HTTP response is returned.
type CommandReceipt struct {
	IdempotencyKey string
	ActionKey      string
	ResourceKey    string
	RequestHash    string
	Status         string
	HTTPStatus     int
	ResponseJSON   []byte
}

type CommandReceiptStore interface {
	ClaimCommand(context.Context, CommandReceipt) (CommandReceipt, bool, error)
	CompleteCommand(context.Context, string, string, int, []byte) error
}
