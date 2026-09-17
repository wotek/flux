package flux

import "fmt"

// Aggregate defines the core contract for a domain aggregate.
// It leverages Go 1.26 self-referencing constraints for reflection-free instantiation.
type Aggregate[A Aggregate[A, E], E Event] interface {
	// Identifier returns the globally unique identifier for this aggregate.
	Identifier() Identifier

	// Changeset returns the tracker for new, uncommitted events.
	Changeset() Changeset[E]

	// Revision returns the aggregate's current sequence number.
	Revision() uint64

	// FromEvents replays historical events to reconstitute the aggregate's state.
	// It accepts a StreamIterator (which yields Envelopes) to allow the aggregate
	// to synchronize its internal Revision alongside applying the event payloads.
	FromEvents(events StreamIterator) error

	// New creates a new, empty instance of the aggregate.
	// This is called on a nil pointer by the framework during loading.
	// The repository provides the Stream so the aggregate can initialize its identity.
	New(stream Stream) A
}

// revisionSetter is an internal framework capability interface used to safely
// mutate aggregate revisions after successful event persistence or snapshot hydration.
type revisionSetter interface {
	setRevision(rev uint64)
}

// AggregateRoot is an embeddable struct providing the foundational boilerplate
// for any domain aggregate (composition over inheritance).
type AggregateRoot[E Event] struct {
	stream    Stream
	revision  uint64
	changeset Changeset[E]

	// apply is a closure/method provided by the concrete aggregate to mutate its state.
	apply func(E) error
}

// NewAggregateRoot initializes the boilerplate. The concrete aggregate passes its Apply method.
// Note: Revisions are not incremented when recording new events to the changeset, only when
// replaying from the EventStore or after successful persistence.
func NewAggregateRoot[E Event](stream Stream, changeset Changeset[E], apply func(E) error) AggregateRoot[E] {
	return AggregateRoot[E]{
		stream:    stream,
		changeset: changeset,
		apply:     apply,
	}
}

// Identifier returns the underlying globally unique identifier.
func (a *AggregateRoot[E]) Identifier() Identifier {
	return a.stream.Identifier
}

// Changeset returns the tracked uncommitted events.
func (a *AggregateRoot[E]) Changeset() Changeset[E] {
	return a.changeset
}

// Revision returns the aggregate's current sequence number.
func (a *AggregateRoot[E]) Revision() uint64 {
	return a.revision
}

// setRevision is an internal framework helper to safely update the revision after persistence or snapshot hydration.
func (a *AggregateRoot[E]) setRevision(rev uint64) {
	a.revision = rev
}

// FromEvents iterates over the StreamIterator, type-asserts the generic Event
// into the aggregate's specific Event type E, applies it, and updates the revision.
func (a *AggregateRoot[E]) FromEvents(events StreamIterator) error {
	for env, err := range events {
		if err != nil {
			return err
		}

		// Ensure the event conforms to this aggregate's specific event type constraint.
		domainEvent, ok := env.Event.(E)
		if !ok {
			return fmt.Errorf("%w: aggregate %s cannot apply %T", ErrInvalidEvent, a.stream.Identifier.String(), env.Event)
		}

		if err := a.apply(domainEvent); err != nil {
			return err
		}
		// Synchronize aggregate revision with the envelope's revision
		a.revision = env.Revision
	}
	return nil
}
