package store

import (
	"context"
	"fmt"
	"sync"

	"github.com/wotek/flux"
)

var _ flux.SnapshotStore[any] = (*SnapshotStore[any])(nil)

// SnapshotStore is an in-memory implementation of [flux.SnapshotStore].
// It is thread-safe and suitable for unit testing and local development.
type SnapshotStore[S any] struct {
	mu        sync.RWMutex
	snapshots map[string]flux.Snapshot[S]
}

// New creates a new in-memory [SnapshotStore].
func New[S any]() *SnapshotStore[S] {
	return &SnapshotStore[S]{
		snapshots: make(map[string]flux.Snapshot[S]),
	}
}

// NewSnapshotStore is an alias for [New] to maintain explicit constructor naming.
func NewSnapshotStore[S any]() *SnapshotStore[S] {
	return New[S]()
}

// Load retrieves the latest snapshot for the specified stream from memory.
// Returns [flux.ErrSnapshotNotFound] if no snapshot exists for the stream.
func (s *SnapshotStore[S]) Load(ctx context.Context, stream flux.Stream) (flux.Snapshot[S], error) {
	if err := ctx.Err(); err != nil {
		return flux.Snapshot[S]{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	streamID := stream.Identifier.String()
	snap, ok := s.snapshots[streamID]
	if !ok {
		return flux.Snapshot[S]{}, fmt.Errorf("loading snapshot for %q: %w", stream.Identifier, flux.ErrSnapshotNotFound)
	}

	return snap, nil
}

// Save persists a snapshot for the specified stream in memory.
// If an existing snapshot exists with a higher revision, the older snapshot is ignored
// to prevent out-of-order writes from regressing state.
func (s *SnapshotStore[S]) Save(ctx context.Context, stream flux.Stream, snap flux.Snapshot[S]) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	streamID := stream.Identifier.String()
	if existing, exists := s.snapshots[streamID]; exists && snap.Revision < existing.Revision {
		return nil
	}

	s.snapshots[streamID] = snap
	return nil
}
