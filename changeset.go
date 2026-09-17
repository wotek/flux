package flux

// Changeset captures all uncommitted events that have been applied to an aggregate.
type Changeset[E Event] interface {
	// Record appends a new event to the changeset.
	Record(event E)

	// Clear empties the changeset (typically called after successful persistence).
	Clear()

	// HasChanges returns true if there are uncommitted events.
	HasChanges() bool

	// Events returns the list of uncommitted events.
	Events() []E
}

// sliceChangeset is a default, memory-backed implementation of Changeset.
type sliceChangeset[E Event] struct {
	events []E
}

// NewChangeset provides a default, slice-backed implementation of the Changeset interface.
func NewChangeset[E Event]() Changeset[E] {
	return &sliceChangeset[E]{
		events: make([]E, 0),
	}
}

func (c *sliceChangeset[E]) Record(event E) {
	c.events = append(c.events, event)
}

func (c *sliceChangeset[E]) Clear() {
	c.events = c.events[:0]
}

func (c *sliceChangeset[E]) HasChanges() bool {
	return len(c.events) > 0
}

func (c *sliceChangeset[E]) Events() []E {
	return c.events
}
