package store

import (
	"context"
	"sync"

	"github.com/wotek/flux"
	"github.com/wotek/flux/projection"
)

var _ projection.Store = (*ProjectionStore)(nil)

// ProjectionStore is an in-memory implementation of projection.Store.
type ProjectionStore struct {
	mu        sync.RWMutex
	positions map[string]uint64
}

// New creates a new in-memory projection store.
func New() *ProjectionStore {
	return &ProjectionStore{
		positions: make(map[string]uint64),
	}
}

// NewProjectionStore is an alias for New to maintain backwards compatibility.
func (s *ProjectionStore) GetPosition(ctx context.Context, id flux.Identifier) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.positions[id.String()], nil
}

func (s *ProjectionStore) Update(ctx context.Context, id flux.Identifier, env flux.Envelope, mutate func(txCtx context.Context) error) error {
	// In a real database, this lock would be a database transaction.
	// We simulate the transaction lock here.
	s.mu.Lock()
	defer s.mu.Unlock()

	// Execute the user's read-model mutation logic
	if err := mutate(ctx); err != nil {
		return err // If it fails, we roll back (in this case, just return and don't update position)
	}

	// Commit the position atomically with the changes
	s.positions[id.String()] = env.Position
	return nil
}
