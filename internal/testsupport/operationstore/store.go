package operationstore

import (
	"context"
	"fmt"
	"strings"
	"sync"

	sharedoperation "github.com/domainry/domainry-foundation/operation"
)

type Store struct {
	mu       sync.Mutex
	receipts map[string]sharedoperation.Receipt
}

func New() *Store {
	return &Store{receipts: map[string]sharedoperation.Receipt{}}
}

func (s *Store) Claim(_ context.Context, command sharedoperation.Command) (sharedoperation.Receipt, bool, error) {
	if err := command.Validate(); err != nil {
		return sharedoperation.Receipt{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := identity(command.Scope, command.Owner, command.Kind, command.IdempotencyKey)
	if existing, ok := s.receipts[key]; ok {
		if existing.Command.RequestFingerprint != command.RequestFingerprint {
			return sharedoperation.Receipt{}, false, sharedoperation.ErrIdempotencyConflict
		}
		return clone(existing), false, nil
	}
	receipt := sharedoperation.Receipt{Command: command, Status: sharedoperation.StatusStarted}
	s.receipts[key] = receipt
	return clone(receipt), true, nil
}

func (s *Store) Complete(_ context.Context, completion sharedoperation.Completion) error {
	if err := completion.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := identity(completion.Scope, completion.Owner, completion.Kind, completion.IdempotencyKey)
	receipt, ok := s.receipts[key]
	if !ok || receipt.Status != sharedoperation.StatusStarted || receipt.Command.RequestFingerprint != completion.RequestFingerprint {
		return fmt.Errorf("shared operation is not executing")
	}
	receipt.Status = sharedoperation.StatusSucceeded
	receipt.Result = append([]byte(nil), completion.Result...)
	s.receipts[key] = receipt
	return nil
}

func identity(scope sharedoperation.Scope, owner, kind, key string) string {
	return strings.Join([]string{scope.WorkspaceID, scope.SystemPurpose, owner, kind, key}, "\x00")
}

func clone(receipt sharedoperation.Receipt) sharedoperation.Receipt {
	receipt.Result = append([]byte(nil), receipt.Result...)
	return receipt
}

var _ sharedoperation.Store = (*Store)(nil)
