package flux

import "errors"

var (
	// ErrAggregateNotFound is returned when an aggregate cannot be loaded from the EventStore.
	ErrAggregateNotFound = errors.New("aggregate not found")

	// ErrSnapshotNotFound is returned by a SnapshotStore when no snapshot exists for a stream.
	ErrSnapshotNotFound = errors.New("snapshot not found")

	// ErrConcurrency is returned by an EventStore when an optimistic concurrency check fails.
	ErrConcurrency = errors.New("optimistic concurrency check failed")

	// ErrInvalidEvent is returned when an aggregate's FromEvents encounters an event type it cannot apply.
	ErrInvalidEvent = errors.New("invalid event type for aggregate")

	// ErrNoHandler is returned by Command and Query buses when no handler is registered for a given type.
	ErrNoHandler = errors.New("no handler registered")

	// ErrInvalidHandlerType is returned when a requested handler signature does not match the registered handler.
	ErrInvalidHandlerType = errors.New("invalid handler type")

	// ErrSnapshotPersistence is returned by SnapshotRepository.Save when events were
	// successfully appended to the EventStore, but saving the snapshot failed.
	// Callers can safely retry Save to re-attempt the snapshot write.
	ErrSnapshotPersistence = errors.New("failed to persist snapshot (events were successfully committed)")

	// ErrMissingRevisionSetter indicates that an aggregate does not implement the internal
	// revisionSetter interface (typically because it failed to embed flux.AggregateRoot).
	ErrMissingRevisionSetter = errors.New("aggregate must embed flux.AggregateRoot to manage revisions")
)
