package counter

import (
	"context"
	"sync"
)

// MemoryStore provides an in-memory, thread-safe implementation of [Store].
type MemoryStore struct {
	mu       sync.RWMutex
	active   int
	archived int
	removed  int
}

// NewMemoryStore constructs an initialized [MemoryStore].
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

// MemoryCounterStore is an alias for [MemoryStore].
type MemoryCounterStore = MemoryStore

// NewMemoryCounterStore is an alias for [NewMemoryStore].
func NewMemoryCounterStore() *MemoryStore {
	return NewMemoryStore()
}

// IncrementActive modifies the active task counter by the specified delta.
func (s *MemoryStore) IncrementActive(_ context.Context, delta int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active += delta
	return nil
}

// IncrementArchived modifies the archived task counter by the specified delta.
func (s *MemoryStore) IncrementArchived(_ context.Context, delta int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.archived += delta
	return nil
}

// IncrementRemoved modifies the removed task counter by the specified delta.
func (s *MemoryStore) IncrementRemoved(_ context.Context, delta int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removed += delta
	return nil
}

// GetCounter retrieves the current [Counter] statistics snapshot.
func (s *MemoryStore) GetCounter(_ context.Context) (Counter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Counter{
		Active:   s.active,
		Archived: s.archived,
		Removed:  s.removed,
	}, nil
}
