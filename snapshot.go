package flux

import "context"

// Snapshotable defines how an aggregate exposes and restores its internal state using the Memento pattern.
type Snapshotable[S any] interface {
	// Snapshot captures and returns the current point-in-time state of the aggregate.
	Snapshot() S

	// With restores the aggregate's internal state from the provided snapshot state.
	With(state S)
}

// Snapshot represents a captured point-in-time state of an Aggregate at a specific revision.
type Snapshot[S any] struct {
	State    S
	Revision uint64
}

// SnapshotStore defines the persistence contract for storing and retrieving aggregate snapshots.
type SnapshotStore[S any] interface {
	// Load retrieves the latest snapshot for the specified stream.
	Load(ctx context.Context, stream Stream) (Snapshot[S], error)

	// Save persists a snapshot for the specified stream.
	Save(ctx context.Context, stream Stream, snap Snapshot[S]) error
}
