package store

import (
	"context"
	"sync"

	"github.com/wotek/flux"
	"github.com/wotek/flux/checkpoint"
	checkpointstore "github.com/wotek/flux/checkpoint/store"
	"github.com/wotek/flux/projection"
)

var _ projection.Store = (*ProjectionStore)(nil)

// Option configures an in-memory [ProjectionStore].
type Option func(*ProjectionStore)

// WithCheckpointStore sets a custom [checkpoint.Store] to maintain stream positions.
//
// This option is intended for sharing or testing an in-memory (or test double)
// [checkpoint.Store] across components in tests and local prototypes.
//
// Caution: Injecting a durable remote checkpoint store (such as MySQL or Redis)
// into this in-memory projection store holds a process-local mutex across SetPosition
// and serializes all updates across goroutines. It does NOT provide same-database
// transactional atomicity between read-model mutations and checkpoint persistence.
// For durable same-database mutate and checkpoint atomicity, use a dedicated
// transactional projection store (e.g. MySQL projection.Store).
//
// cs must be non-nil; passing nil panics immediately.
func WithCheckpointStore(cs checkpoint.Store) Option {
	if cs == nil {
		panic("projection: checkpoint store is required; pass checkpoint/store.New() for in-memory or a durable store for production")
	}
	return func(s *ProjectionStore) {
		s.checkpoints = cs
	}
}

// ProjectionStore is an in-memory implementation of [projection.Store] intended
// for unit tests and local prototypes. It composes a [checkpoint.Store] to track
// consumer progress.
//
// Concurrency & Atomicity:
// Update synchronizes execution using a process-local mutex across mutate and
// SetPosition. This is suitable for in-memory state, but does not provide
// distributed or transactional atomicity when composed with external durable stores.
type ProjectionStore struct {
	mu          sync.Mutex
	checkpoints checkpoint.Store
}

// New creates a new in-memory projection store for tests and prototypes.
// If no [WithCheckpointStore] option is provided, it defaults to [checkpoint/store.New].
// If an option sets a nil checkpoint store, New panics.
func New(opts ...Option) *ProjectionStore {
	s := &ProjectionStore{
		checkpoints: checkpointstore.New(),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.checkpoints == nil {
		panic("projection: checkpoint store is required; pass checkpoint/store.New() for in-memory or a durable store for production")
	}
	return s
}

// NewProjectionStore is an alias for [New] to maintain explicit constructor naming.
func NewProjectionStore(opts ...Option) *ProjectionStore {
	return New(opts...)
}

// GetPosition loads the consumer's global stream cursor from the composed checkpoint store.
func (s *ProjectionStore) GetPosition(ctx context.Context, id flux.Identifier) (uint64, error) {
	return s.checkpoints.GetPosition(ctx, id)
}

// Update runs mutate inside a simulated transaction boundary and, on success,
// advances the checkpoint to env.Position. If mutate returns an error, the checkpoint
// does not advance.
func (s *ProjectionStore) Update(ctx context.Context, id flux.Identifier, env flux.Envelope, mutate func(txCtx context.Context) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Execute the user's read-model mutation logic
	if err := mutate(ctx); err != nil {
		return err // If it fails, do not advance position
	}

	// Advance position via the composed checkpoint store
	return s.checkpoints.SetPosition(ctx, id, env.Position)
}
