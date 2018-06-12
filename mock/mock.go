// Package mock provides mocks for the wallet domain to facilitate testing.
// For example, for most cases we do not need Kafka implementation of a transfer service
// to be used in HTTP API testing.
package mock

import (
	"context"

	wallet "github.com/marselester/distributed-payment"
)

// TransferService is a mock that implements wallet.TransferService.
type TransferService struct {
	CreateTransferFn     func(ctx context.Context, t *wallet.Transfer) error
	CreateTransferCalled bool
	FromOffsetFn         func(ctx context.Context, partition int32, offset int64) (<-chan *wallet.Transfer, <-chan error)
	FromOffsetCalled     bool
}

// CreateTransfer calls CreateTransferFn and sets CreateTransferCalled = true for tests
// to inspect the mock.
func (s *TransferService) CreateTransfer(ctx context.Context, t *wallet.Transfer) error {
	s.CreateTransferCalled = true
	if s.CreateTransferFn == nil {
		return nil
	}
	return s.CreateTransferFn(ctx, t)
}

// FromOffset calls FromOffsetFn and sets FromOffsetCalled = true for tests to inspect the mock.
func (s *TransferService) FromOffset(ctx context.Context, partition int32, offset int64) (<-chan *wallet.Transfer, <-chan error) {
	s.FromOffsetCalled = true
	return s.FromOffsetFn(ctx, partition, offset)
}
