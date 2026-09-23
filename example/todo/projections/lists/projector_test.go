package lists_test

import (
	"context"
	"testing"
	"time"

	"github.com/wotek/flux"
	eventstore "github.com/wotek/flux/event/store"
	"github.com/wotek/flux/example/todo/events"
	"github.com/wotek/flux/example/todo/projections/lists"
	projectionstore "github.com/wotek/flux/projection/store"
)

func TestListsProjector_TracksMultipleLists(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	eventStore := eventstore.New()
	projStore := projectionstore.New()
	listsStore := lists.NewMemoryStore()

	projID := flux.MustParseIdentifier("urn:todo:prod:projections:1:lists:unit")
	projector := lists.NewProjector(projID, eventStore, projStore, listsStore)

	go func() {
		_ = projector.Start(ctx)
	}()

	stream1 := flux.Stream{Identifier: flux.MustParseIdentifier("urn:todo:prod:lists:1:list:work")}
	stream2 := flux.Stream{Identifier: flux.MustParseIdentifier("urn:todo:prod:lists:1:list:personal")}
	baseCtx := flux.NewContext(ctx, flux.Actor{}, flux.Identifier{}, flux.Identifier{})

	envelopes1 := []flux.Envelope{
		{Identifier: flux.MustParseIdentifier("urn:todo:prod:events:1:event:1"), Stream: stream1, Event: events.ListCreated{Title: "Work Tasks"}},
		{Identifier: flux.MustParseIdentifier("urn:todo:prod:events:1:event:2"), Stream: stream1, Event: events.TaskAdded{Task: "Task 1"}},
		{Identifier: flux.MustParseIdentifier("urn:todo:prod:events:1:event:3"), Stream: stream1, Event: events.TaskAdded{Task: "Task 2"}},
		{Identifier: flux.MustParseIdentifier("urn:todo:prod:events:1:event:4"), Stream: stream1, Event: events.TasksDone{Tasks: []string{"Task 1"}}},
	}

	envelopes2 := []flux.Envelope{
		{Identifier: flux.MustParseIdentifier("urn:todo:prod:events:1:event:5"), Stream: stream2, Event: events.ListCreated{Title: "Personal"}},
		{Identifier: flux.MustParseIdentifier("urn:todo:prod:events:1:event:6"), Stream: stream2, Event: events.TaskAdded{Task: "Groceries"}},
	}

	if err := eventStore.Append(baseCtx, stream1, 0, envelopes1); err != nil {
		t.Fatalf("failed to append stream 1: %v", err)
	}
	if err := eventStore.Append(baseCtx, stream2, 0, envelopes2); err != nil {
		t.Fatalf("failed to append stream 2: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var allLists []lists.ListSummary
	var err error

	for time.Now().Before(deadline) {
		allLists, err = listsStore.GetLists(ctx)
		if err == nil && len(allLists) == 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	if err != nil {
		t.Fatalf("failed to get lists: %v", err)
	}
	if len(allLists) != 2 {
		t.Fatalf("expected 2 lists, got %d", len(allLists))
	}
}
