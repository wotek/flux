package flux

import "context"

// EventStore defines the interface for an append-only log of events.
type EventStore interface {
	// Append adds new events to a specific stream, enforcing optimistic concurrency
	// via the expected stream revision.
	Append(ctx context.Context, stream Stream, expectedRevision uint64, events []Envelope) error

	// Read retrieves all events for a specific stream.
	// It returns a StreamIterator (iter.Seq2) to efficiently iterate over large streams.
	Read(ctx context.Context, stream Stream) (StreamIterator, error)

	// Stream retrieves events from the global event log starting from the given position.
	// This is used by Projectors and Sagas to tail the entire system's events.
	Stream(ctx context.Context, position uint64) (StreamIterator, error)
}
