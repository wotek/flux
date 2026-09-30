package store

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/wotek/flux"
)

var _ flux.EventStore = (*EventStore)(nil)

// EventStore is an in-memory implementation of flux.EventStore.
// It is thread-safe and suitable for unit testing and local development.
type EventStore struct {
	mu           sync.RWMutex
	streams      map[string][]flux.Envelope
	globalStream []flux.Envelope
}

// New creates a new in-memory event store.
func New() *EventStore {
	return &EventStore{
		streams:      make(map[string][]flux.Envelope),
		globalStream: make([]flux.Envelope, 0),
	}
}

// NewEventStore is an alias for New to maintain backwards compatibility.
func NewEventStore() *EventStore {
	return New()
}

// Append adds new events to a specific stream, enforcing optimistic concurrency.
func (s *EventStore) Append(ctx context.Context, stream flux.Stream, expectedRevision uint64, events []flux.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if len(events) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	streamID := stream.Identifier.String()
	existing := s.streams[streamID]

	// Check optimistic concurrency constraint
	var currentRevision uint64
	if len(existing) > 0 {
		currentRevision = existing[len(existing)-1].Revision
	}

	if currentRevision != expectedRevision {
		return fmt.Errorf("%w: expected revision %d, got %d", flux.ErrConcurrency, expectedRevision, currentRevision)
	}

	// Append events
	for i, env := range events {
		env.Stream = stream
		env.Revision = currentRevision + uint64(i) + 1
		env.Position = uint64(len(s.globalStream) + 1)

		if env.CreatedAt.IsZero() {
			env.CreatedAt = time.Now()
		}

		s.streams[streamID] = append(s.streams[streamID], env)
		s.globalStream = append(s.globalStream, env)
	}

	return nil
}

// Read retrieves events for a specific stream starting from the given position (revision) via an iterator.
// Passing 0 reads the entire stream from the beginning. Events with Revision <= fromRevision are skipped.
func (s *EventStore) Read(ctx context.Context, stream flux.Stream, fromRevision uint64) (flux.StreamIterator, error) {
	s.mu.RLock()
	streamID := stream.Identifier.String()
	rawEvents := s.streams[streamID]
	events := make([]flux.Envelope, 0, len(rawEvents))
	for _, env := range rawEvents {
		if env.Revision > fromRevision {
			events = append(events, env)
		}
	}
	s.mu.RUnlock()

	// Return the iter.Seq2 iterator function
	return func(yield func(flux.Envelope, error) bool) {
		for _, e := range events {
			if !yield(e, nil) {
				return
			}
		}
	}, nil
}

// Stream retrieves events from the global event log starting from the given position.
func (s *EventStore) Stream(ctx context.Context, position uint64) (flux.StreamIterator, error) {
	s.mu.RLock()
	// Filter global stream for events with Position > position
	// Since Position is 1-indexed and strictly increasing, we can just slice it if position < len
	var events []flux.Envelope
	if position < uint64(len(s.globalStream)) {
		events = make([]flux.Envelope, uint64(len(s.globalStream))-position)
		copy(events, s.globalStream[position:])
	}
	s.mu.RUnlock()

	return func(yield func(flux.Envelope, error) bool) {
		for _, e := range events {
			if !yield(e, nil) {
				return
			}
		}
	}, nil
}

// Find returns the envelope for the given stream and event identifier.
// If no matching event exists in that stream, it returns an error wrapping [flux.ErrEventNotFound].
func (s *EventStore) Find(ctx context.Context, stream flux.Stream, eventID flux.Identifier) (flux.Envelope, error) {
	if err := ctx.Err(); err != nil {
		return flux.Envelope{}, err
	}

	if eventID.IsEmpty() {
		return flux.Envelope{}, fmt.Errorf("%w: empty event identifier", flux.ErrEventNotFound)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	streamID := stream.Identifier.String()
	events, exists := s.streams[streamID]
	if !exists {
		return flux.Envelope{}, fmt.Errorf("%w: stream %q not found", flux.ErrEventNotFound, streamID)
	}

	for _, env := range events {
		if env.Identifier == eventID {
			copied := env
			if env.Metadata != nil {
				copied.Metadata = maps.Clone(env.Metadata)
			}
			return copied, nil
		}
	}

	return flux.Envelope{}, fmt.Errorf("%w: event %q not found in stream %q", flux.ErrEventNotFound, eventID.String(), streamID)
}
