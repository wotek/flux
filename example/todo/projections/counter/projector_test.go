package counter_test

import (
	"context"
	"testing"
	"time"

	"github.com/wotek/flux"
	eventstore "github.com/wotek/flux/event/store"
	"github.com/wotek/flux/example/todo/events"
	"github.com/wotek/flux/example/todo/projections/counter"
	projectionstore "github.com/wotek/flux/projection/store"
)

func TestCounterProjector_UpdatesMetrics(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	eventStore := eventstore.New()
	projStore := projectionstore.New()
	statsStore := counter.NewMemoryStore()

	projID := flux.NewIdentifierFromString("urn:todo:prod:projections:1:counter:unit")
	projector := counter.NewProjector(projID, eventStore, projStore, statsStore)

	go func() {
		_ = projector.Start(ctx)
	}()

	stream := flux.Stream{Identifier: flux.NewIdentifierFromString("urn:todo:prod:lists:1:list:unit-test")}
	baseCtx := flux.NewContext(ctx, flux.Actor{}, flux.Identifier{}, flux.Identifier{})

	// Append events directly into the event store
	envelopes := []flux.Envelope{
		{Identifier: flux.NewIdentifierFromString("urn:todo:prod:events:1:event:1"), Stream: stream, Event: events.TaskAdded{Task: "A"}},
		{Identifier: flux.NewIdentifierFromString("urn:todo:prod:events:1:event:2"), Stream: stream, Event: events.TaskAdded{Task: "B"}},
		{Identifier: flux.NewIdentifierFromString("urn:todo:prod:events:1:event:3"), Stream: stream, Event: events.TaskAdded{Task: "C"}},
		{Identifier: flux.NewIdentifierFromString("urn:todo:prod:events:1:event:4"), Stream: stream, Event: events.TaskRemoved{Task: "A"}},
		{Identifier: flux.NewIdentifierFromString("urn:todo:prod:events:1:event:5"), Stream: stream, Event: events.TasksDone{Tasks: []string{"B"}}},
	}

	if err := eventStore.Append(baseCtx, stream, 0, envelopes); err != nil {
		t.Fatalf("failed to append envelopes: %v", err)
	}

	// Poll until projector reaches expected counts: active=1 (C), archived=1 (B), removed=1 (A)
	deadline := time.Now().Add(2 * time.Second)
	var snapshot counter.Counter
	var err error
	for time.Now().Before(deadline) {
		snapshot, err = statsStore.GetCounter(ctx)
		if err == nil && snapshot.Active == 1 && snapshot.Archived == 1 && snapshot.Removed == 1 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	if err != nil {
		t.Fatalf("failed to read counter snapshot: %v", err)
	}

	if snapshot.Active != 1 {
		t.Errorf("active mismatch: got %d, want 1", snapshot.Active)
	}
	if snapshot.Archived != 1 {
		t.Errorf("archived mismatch: got %d, want 1", snapshot.Archived)
	}
	if snapshot.Removed != 1 {
		t.Errorf("removed mismatch: got %d, want 1", snapshot.Removed)
	}
}
