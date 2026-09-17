package projections

import (
	"context"
	"maps"
	"slices"
	"sync"
)

// Store defines persistence operations for the catalog read model.
type Store interface {
	Get(ctx context.Context, id string) (ProductView, bool, error)
	Save(ctx context.Context, view ProductView) error
	List(ctx context.Context) ([]ProductView, error)
}

// MemoryStore provides an in-memory implementation of [Store].
type MemoryStore struct {
	mu       sync.RWMutex
	products map[string]ProductView
}

// NewMemoryStore creates a new in-memory catalog store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		products: make(map[string]ProductView),
	}
}

// Get retrieves a [ProductView] by product ID.
func (s *MemoryStore) Get(ctx context.Context, id string) (ProductView, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	view, exists := s.products[id]
	return view, exists, nil
}

// Save inserts or updates a [ProductView].
func (s *MemoryStore) Save(ctx context.Context, view ProductView) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.products[view.ID] = view
	return nil
}

// List returns all product views sorted by product ID.
func (s *MemoryStore) List(ctx context.Context) ([]ProductView, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	views := slices.Collect(maps.Values(s.products))
	slices.SortFunc(views, func(a, b ProductView) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	return views, nil
}
