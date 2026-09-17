package flux

import "fmt"

// SnapshotAggregate ensures the aggregate type implements both [Aggregate] and [Snapshotable].
type SnapshotAggregate[A Aggregate[A, E], E Event, S any] interface {
	Aggregate[A, E]
	Snapshotable[S]
}

// SnapshotRepository wraps an [AggregateRepository] to provide snapshot-assisted loading
// and scheduled snapshot saving.
type SnapshotRepository[A SnapshotAggregate[A, E, S], E Event, S any] struct {
	base       *AggregateRepository[A, E]
	store      SnapshotStore[S]
	schedule   SnapshotSchedule[A]
	eventStore EventStore
}

// NewSnapshotRepository constructs a new [SnapshotRepository].
func NewSnapshotRepository[A SnapshotAggregate[A, E, S], E Event, S any](
	base *AggregateRepository[A, E],
	store SnapshotStore[S],
	schedule SnapshotSchedule[A],
	eventStore EventStore,
) *SnapshotRepository[A, E, S] {
	return &SnapshotRepository[A, E, S]{
		base:       base,
		store:      store,
		schedule:   schedule,
		eventStore: eventStore,
	}
}

// Load fetches an aggregate from a snapshot (if available) and catches up with any trailing events.
// If no snapshot exists, it falls back to replaying all events from the beginning.
func (r *SnapshotRepository[A, E, S]) Load(ctx Context, stream Stream) (A, error) {
	var zero A
	var agg A
	var startRevision uint64

	if snap, err := r.store.Load(ctx, stream); err == nil {
		// Rehydrate from snapshot
		agg = zero.New(stream)
		agg.With(snap.State)

		// Safely update the aggregate revision using the internal capability interface
		if setter, ok := any(agg).(revisionSetter); ok {
			setter.setRevision(snap.Revision)
		}
		startRevision = snap.Revision
	} else {
		// Fallback to purely event-sourced
		agg = zero.New(stream)
	}

	// Catch up with trailing events
	events, err := r.eventStore.Read(ctx, stream, startRevision)
	if err != nil {
		return zero, fmt.Errorf("reading event stream: %w", err)
	}

	if err := agg.FromEvents(events); err != nil {
		return zero, fmt.Errorf("replaying events: %w", err)
	}

	// Verify the stream actually existed (revision > 0)
	// A new, uninitialized aggregate will have revision 0.
	if agg.Revision() == 0 {
		return zero, fmt.Errorf("%w: %s", ErrAggregateNotFound, stream.Identifier.String())
	}

	return agg, nil
}

// Save persists uncommitted events to the underlying event store and evaluates the snapshot schedule.
// If the schedule matches, a new snapshot is captured and persisted.
func (r *SnapshotRepository[A, E, S]) Save(ctx Context, aggregate A) error {
	if err := r.base.Save(ctx, aggregate); err != nil {
		return err
	}

	if r.schedule == nil || !r.schedule.Test(aggregate) {
		return nil
	}

	snap := Snapshot[S]{
		State:    aggregate.Snapshot(),
		Revision: aggregate.Revision(),
	}

	stream := Stream{Identifier: aggregate.Identifier()}
	if err := r.store.Save(ctx, stream, snap); err != nil {
		return fmt.Errorf("saving snapshot: %w", err)
	}

	return nil
}
