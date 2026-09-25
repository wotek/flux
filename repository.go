package flux

import (
	"fmt"
	"time"
	"github.com/oklog/ulid/v2"
)

// AggregateRepository provides the standard unit-of-work interface for Event Sourced aggregates.
type AggregateRepository[A Aggregate[A, E], E Event] struct {
	eventStore EventStore
}

// NewAggregateRepository creates a new repository for a specific Aggregate and Event type.
func NewAggregateRepository[A Aggregate[A, E], E Event](eventStore EventStore) *AggregateRepository[A, E] {
	return &AggregateRepository[A, E]{
		eventStore: eventStore,
	}
}

// Load fetches events for the provided aggregate Stream from the Event Store and replays them.
// It leverages the Aggregate interface's New(stream Stream) method to instantiate the object internally.
func (r *AggregateRepository[A, E]) Load(ctx Context, stream Stream) (A, error) {
	var zero A
	agg := zero.New(stream)

	events, err := r.eventStore.Read(ctx, stream, 0)
	if err != nil {
		return zero, err
	}

	if err := agg.FromEvents(events); err != nil {
		return zero, fmt.Errorf("failed to replay events: %w", err)
	}

	// Verify the stream actually existed (revision > 0)
	// A new, uninitialized aggregate will have revision 0.
	if agg.Revision() == 0 {
		return zero, fmt.Errorf("%w: %s", ErrAggregateNotFound, stream.Identifier.String())
	}

	return agg, nil
}

// Save persists the uncommitted events from the aggregate's Changeset to the Event Store.
func (r *AggregateRepository[A, E]) Save(ctx Context, aggregate A) error {
	changeset := aggregate.Changeset()
	if !changeset.HasChanges() {
		return nil
	}

	uncommitted := changeset.Events()
	envelopes := make([]Envelope, 0, len(uncommitted))

	baseRevision := aggregate.Revision()

	for i, event := range uncommitted {
		// Provide basic metadata. The EventStore sets the actual global Position and finalized Revision.
		env := Envelope{
			Identifier:            NewIdentifier("", "", "stream", "", "event", ulid.Make().String(), ""),
			Stream:                Stream{Identifier: aggregate.Identifier()},
			Revision:              baseRevision + uint64(i) + 1,
			Event:                 event,
			Actor:                 ctx.Actor(),
			CorrelationIdentifier: ctx.CorrelationIdentifier(),
			CausationIdentifier:   ctx.CausationIdentifier(),
			CreatedAt:             time.Now(),
		}
		envelopes = append(envelopes, env)
	}

	stream := Stream{Identifier: aggregate.Identifier()}
	err := r.eventStore.Append(ctx, stream, baseRevision, envelopes)
	if err != nil {
		return fmt.Errorf("failed to append events to store: %w", err)
	}

	// Safely update the aggregate revision using the internal capability interface
	if setter, ok := any(aggregate).(revisionSetter); ok {
		setter.setRevision(baseRevision + uint64(len(uncommitted)))
	}

	changeset.Clear()
	return nil
}
