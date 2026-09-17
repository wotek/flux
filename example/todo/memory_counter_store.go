package todo

import (
	"context"
	"sync"
)

// MemoryCounterStore provides an in-memory, thread-safe implementation of [CounterStore].
type MemoryCounterStore struct {
	mu       sync.RWMutex
	active   int
	archived int
	removed  int
}

// NewMemoryCounterStore constructs an initialized [MemoryCounterStore].
func NewMemoryCounterStore() *MemoryCounterStore {
	return &MemoryCounterStore{}
}

// IncrementActive modifies the active task counter by the specified delta.
func (s *MemoryCounterStore) IncrementActive(_ context.Context, delta int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active += delta
	return nil
}

// IncrementArchived modifies the archived task counter by the specified delta.
func (s *MemoryCounterStore) IncrementArchived(_ context.Context, delta int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.archived += delta
	return nil
}

// IncrementRemoved modifies the removed task counter by the specified delta.
func (s *MemoryCounterStore) IncrementRemoved(_ context.Context, delta int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removed += delta
	return nil
}

// GetCounter retrieves the current [Counter] statistics snapshot.
func (s *MemoryCounterStore) GetCounter(_ context.Context) (Counter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Counter{
		Active:   s.active,
		Archived: s.archived,
		Removed:  s.removed,
	}, nil
}
