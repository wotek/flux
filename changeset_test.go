package flux

import "testing"

type mockEvent struct {
	name string
}

func (m mockEvent) Name() string { return m.name }

func TestChangeset(t *testing.T) {
	t.Parallel()
	cs := NewChangeset[Event]()

	if cs.HasChanges() {
		t.Errorf("expected new changeset to have no changes")
	}

	e1 := mockEvent{name: "Event1"}
	e2 := mockEvent{name: "Event2"}

	cs.Record(e1)
	cs.Record(e2)

	if !cs.HasChanges() {
		t.Errorf("expected changeset to have changes")
	}

	events := cs.Events()
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	commit := cs.Events()
	if len(commit) != 2 {
		t.Fatalf("expected 2 committed events, got %d", len(commit))
	}

	cs.Clear()
	if cs.HasChanges() {
		t.Errorf("expected changeset to be empty after Clear")
	}
	if len(cs.Events()) != 0 {
		t.Errorf("expected 0 events after Clear, got %d", len(cs.Events()))
	}
}

func TestChangeset_DefensiveCopy(t *testing.T) {
	t.Parallel()

	cs := NewChangeset[Event]()
	e1 := mockEvent{name: "Event1"}
	cs.Record(e1)

	events := cs.Events()
	events[0] = mockEvent{name: "Mutated"}

	if cs.Events()[0].Name() != "Event1" {
		t.Errorf("expected internal event to remain 'Event1', got %q", cs.Events()[0].Name())
	}

	// Also verify that holding a reference across Clear doesn't corrupt the returned slice
	held := cs.Events()
	cs.Clear()
	if len(held) != 1 || held[0].Name() != "Event1" {
		t.Errorf("expected held slice to remain intact after Clear")
	}
}

