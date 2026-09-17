package inmemory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wotek/flux"
)

// EventStore is an in-memory implementation of flux.EventStore.
// It is thread-safe and suitable for unit testing and local development.
type EventStore struct {
	mu           sync.RWMutex
	streams      map[string][]flux.Envelope
	globalStream []flux.Envelope
}

// NewEventStore creates a new in-memory event store.
func NewEventStore() *EventStore {
	return &EventStore{
		streams:      make(map[string][]flux.Envelope),
		globalStream: make([]flux.Envelope, 0),
	}
}

// Append adds new events to a specific stream, enforcing optimistic concurrency.
func (s *EventStore) Append(ctx context.Context, stream flux.Stream, expectedRevision uint64, events []flux.Envelope) error {
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
		return fmt.Errorf("concurrency error: expected revision %d, got %d", expectedRevision, currentRevision)
	}

	// Append events
	for i, env := range events {
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

// Read retrieves all events for a specific stream via an iterator.
func (s *EventStore) Read(ctx context.Context, stream flux.Stream) (flux.StreamIterator, error) {
	s.mu.RLock()
	streamID := stream.Identifier.String()
	// Create a copy of the slice so we can release the lock immediately
	events := make([]flux.Envelope, len(s.streams[streamID]))
	copy(events, s.streams[streamID])
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
