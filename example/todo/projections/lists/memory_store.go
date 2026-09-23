package lists

import (
	"context"
	"slices"
	"strings"
	"sync"

	"github.com/wotek/flux"
	"github.com/wotek/flux/projection"
)

var _ Store = (*MemoryStore)(nil)

// MemoryStore is an in-memory, thread-safe implementation of [Store].
type MemoryStore struct {
	mu    sync.RWMutex
	lists map[string]*ListSummary
}

// NewMemoryStore constructs a new [MemoryStore].
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		lists: make(map[string]*ListSummary),
	}
}

// UpsertList creates or updates the title of a list.
func (s *MemoryStore) UpsertList(_ projection.Context, id flux.Identifier, title string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	idStr := id.String()
	item, exists := s.lists[idStr]
	if !exists {
		s.lists[idStr] = &ListSummary{
			Identifier: idStr,
			Title:      title,
		}
		return nil
	}

	if title != "" {
		item.Title = title
	}
	return nil
}

// UpdateCounts adjusts the active and archived counts for a list.
func (s *MemoryStore) UpdateCounts(_ projection.Context, id flux.Identifier, deltaActive, deltaArchived int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	idStr := id.String()
	item, exists := s.lists[idStr]
	if !exists {
		item = &ListSummary{
			Identifier: idStr,
			Title:      "Untitled List",
		}
		s.lists[idStr] = item
	}

	item.Active += deltaActive
	if item.Active < 0 {
		item.Active = 0
	}
	item.Archived += deltaArchived
	if item.Archived < 0 {
		item.Archived = 0
	}
	return nil
}

// GetLists returns a sorted copy of all known lists.
func (s *MemoryStore) GetLists(_ context.Context) ([]ListSummary, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]ListSummary, 0, len(s.lists))
	for _, item := range s.lists {
		result = append(result, *item)
	}

	slices.SortFunc(result, func(a, b ListSummary) int {
		return strings.Compare(a.Identifier, b.Identifier)
	})

	return result, nil
}
