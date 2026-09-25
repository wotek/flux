package flux

import (
	"errors"
	"fmt"
	"testing"
)

// Define a test-specific domain event interface.
type TestEvent interface {
	Event
	isTestEvent()
}

type CounterIncremented struct{}

func (e CounterIncremented) Name() string { return "CounterIncremented" }
func (e CounterIncremented) isTestEvent() {}

type CounterDecremented struct{}

func (e CounterDecremented) Name() string { return "CounterDecremented" }
func (e CounterDecremented) isTestEvent() {}

// Define a concrete Aggregate for testing.
type CounterAggregate struct {
	AggregateRoot[TestEvent]
	Count int
}

// Implement the New() requirement for the Aggregate constraint.
func (c *CounterAggregate) New(stream Stream) *CounterAggregate {
	return NewCounterAggregate(stream)
}

func NewCounterAggregate(stream Stream) *CounterAggregate {
	c := &CounterAggregate{}
	c.AggregateRoot = NewAggregateRoot[TestEvent](stream, NewChangeset[TestEvent](), c.apply)
	return c
}

func (c *CounterAggregate) apply(e TestEvent) error {
	switch e.(type) {
	case CounterIncremented:
		c.Count++
	case CounterDecremented:
		c.Count--
	default:
		return fmt.Errorf("unknown event")
	}
	return nil
}

// Helper to create a StreamIterator from a slice of envelopes
func sliceIterator(envelopes []Envelope) StreamIterator {
	return func(yield func(Envelope, error) bool) {
		for _, e := range envelopes {
			if !yield(e, nil) {
				return
			}
		}
	}
}

func TestAggregateRoot_FromEvents(t *testing.T) {
	id := MustParseIdentifier("urn:test::svc:1:counter:abc")
	stream := Stream{Identifier: id}
	agg := NewCounterAggregate(stream)

	envelopes := []Envelope{
		{Revision: 1, Event: CounterIncremented{}},
		{Revision: 2, Event: CounterIncremented{}},
		{Revision: 3, Event: CounterDecremented{}},
		{Revision: 4, Event: CounterIncremented{}},
	}

	err := agg.FromEvents(sliceIterator(envelopes))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if agg.Count != 2 {
		t.Errorf("expected count 2, got %d", agg.Count)
	}

	if agg.Revision() != 4 {
		t.Errorf("expected revision 4, got %d", agg.Revision())
	}

	if agg.Identifier() != id {
		t.Errorf("expected id %v, got %v", id, agg.Identifier())
	}
}

func TestAggregateRoot_FromEvents_WrongType(t *testing.T) {
	id := MustParseIdentifier("urn:test::svc:1:counter:abc")
	stream := Stream{Identifier: id}
	agg := NewCounterAggregate(stream)

	// An event that doesn't implement TestEvent
	wrongEvent := mockEvent{name: "Wrong"}

	envelopes := []Envelope{
		{Revision: 1, Event: wrongEvent},
	}

	err := agg.FromEvents(sliceIterator(envelopes))
	if err == nil {
		t.Fatalf("expected error applying wrong event type")
	}
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected ErrInvalidEvent, got %v", err)
	}
}

func TestAggregateRoot_Changeset(t *testing.T) {
	t.Parallel()
	id := MustParseIdentifier("urn:test::svc:1:counter:abc")
	stream := Stream{Identifier: id}
	agg := NewCounterAggregate(stream)

	cs := agg.Changeset()
	if cs.HasChanges() {
		t.Fatalf("expected empty changeset")
	}

	cs.Record(CounterIncremented{})
	if !cs.HasChanges() {
		t.Fatalf("expected changes")
	}
}

func TestAggregateRoot_Record(t *testing.T) {
	t.Parallel()

	id := MustParseIdentifier("urn:test::svc:1:counter:rec")
	stream := Stream{Identifier: id}
	agg := NewCounterAggregate(stream)

	if agg.Count != 0 {
		t.Fatalf("expected initial Count = 0, got %d", agg.Count)
	}

	// 1. Successful Record mutates state and updates changeset
	if err := agg.Record(CounterIncremented{}); err != nil {
		t.Fatalf("unexpected error recording event: %v", err)
	}
	if agg.Count != 1 {
		t.Errorf("expected Count = 1, got %d", agg.Count)
	}
	if !agg.Changeset().HasChanges() || len(agg.Changeset().Events()) != 1 {
		t.Fatalf("expected 1 uncommitted event in changeset")
	}

	// 2. Failed apply does not append to changeset and returns error
	failingAgg := &CounterAggregate{}
	errFailingApply := errors.New("apply invariant failed")
	failingAgg.AggregateRoot = NewAggregateRoot[TestEvent](stream, NewChangeset[TestEvent](), func(e TestEvent) error {
		return errFailingApply
	})

	if err := failingAgg.Record(CounterIncremented{}); !errors.Is(err, errFailingApply) {
		t.Fatalf("expected %v, got %v", errFailingApply, err)
	}
	if failingAgg.Changeset().HasChanges() {
		t.Errorf("expected changeset to remain empty when apply returns error")
	}
}
