package store

import (
	"context"
	"sync"

	"github.com/wotek/flux"
	"github.com/wotek/flux/checkpoint"
)

var _ checkpoint.Store = (*Store)(nil)

// Store is an in-memory, thread-safe implementation of [checkpoint.Store].
// It is intended for testing, local prototyping, and non-durable tailing workers.
type Store struct {
	mu        sync.RWMutex
	positions map[string]uint64
}

// New creates a new in-memory [Store].
func New() *Store {
	return &Store{
		positions: make(map[string]uint64),
	}
}

// NewStore is an alias for [New] to maintain explicit constructor naming across packages.
func NewStore() *Store {
	return New()
}

// GetPosition returns the stored stream position for the consumer ID.
// If the consumer has no stored position, it returns 0 and nil error.
func (s *Store) GetPosition(ctx context.Context, id flux.Identifier) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.positions[id.String()], nil
}

// SetPosition stores the stream position for the consumer ID using monotonic max semantics.
// If the proposed position is less than or equal to the currently stored position,
// the call succeeds without changing the stored value.
func (s *Store) SetPosition(ctx context.Context, id flux.Identifier, position uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := id.String()
	if current, exists := s.positions[key]; !exists || position > current {
		s.positions[key] = position
	}

	return nil
}
