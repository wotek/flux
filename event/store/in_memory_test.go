package store_test

import (
	"context"
	"errors"
	"testing"
	"time"
	"github.com/oklog/ulid/v2"

	"github.com/wotek/flux"
	eventstore "github.com/wotek/flux/event/store"
)

type dummyEvent struct {
	Value string
}

func (d dummyEvent) Name() string { return "DummyEvent" }

func makeEnvelope(stream flux.Stream, rev uint64, val string) flux.Envelope {
	return flux.Envelope{
		Identifier: flux.NewIdentifier("", "", "stream", "", "event", ulid.Make().String(), ""),
		Stream:     stream,
		Revision:   rev,
		Event:      dummyEvent{Value: val},
		CreatedAt:  time.Now(),
	}
}

func TestEventStore_ReadWithFromRevision(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		fromRevision uint64
		wantCount    int
		wantRevs     []uint64
	}{
		{
			name:         "read from beginning (0)",
			fromRevision: 0,
			wantCount:    3,
			wantRevs:     []uint64{1, 2, 3},
		},
		{
			name:         "read from revision 1",
			fromRevision: 1,
			wantCount:    2,
			wantRevs:     []uint64{2, 3},
		},
		{
			name:         "read from revision 2",
			fromRevision: 2,
			wantCount:    1,
			wantRevs:     []uint64{3},
		},
		{
			name:         "read from revision 3 (latest)",
			fromRevision: 3,
			wantCount:    0,
			wantRevs:     nil,
		},
		{
			name:         "read beyond latest revision",
			fromRevision: 10,
			wantCount:    0,
			wantRevs:     nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := eventstore.New()
			ctx := context.Background()
			stream := flux.Stream{
				Identifier: flux.MustParseIdentifier("urn:test:prod:items:123:item:" + tt.name),
			}

			events := []flux.Envelope{
				makeEnvelope(stream, 1, "first"),
				makeEnvelope(stream, 2, "second"),
				makeEnvelope(stream, 3, "third"),
			}

			if err := store.Append(ctx, stream, 0, events); err != nil {
				t.Fatalf("failed to append events: %v", err)
			}

			iter, err := store.Read(ctx, stream, tt.fromRevision)
			if err != nil {
				t.Fatalf("failed to read stream: %v", err)
			}

			var gotRevs []uint64
			for env, iterErr := range iter {
				if iterErr != nil {
					t.Fatalf("unexpected error during iteration: %v", iterErr)
				}
				gotRevs = append(gotRevs, env.Revision)
			}

			if len(gotRevs) != tt.wantCount {
				t.Fatalf("expected %d events, got %d", tt.wantCount, len(gotRevs))
			}

			for i, rev := range tt.wantRevs {
				if gotRevs[i] != rev {
					t.Errorf("at index %d: expected revision %d, got %d", i, rev, gotRevs[i])
				}
			}
		})
	}
}

func TestEventStore_ConcurrencyError(t *testing.T) {
	t.Parallel()

	store := eventstore.New()
	ctx := context.Background()
	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:test:prod:items:123:item:concurrency"),
	}

	initialEvents := []flux.Envelope{
		makeEnvelope(stream, 1, "first"),
	}

	if err := store.Append(ctx, stream, 0, initialEvents); err != nil {
		t.Fatalf("failed to append initial event: %v", err)
	}

	// Attempt to append with wrong expected revision (0 instead of 1)
	conflictingEvents := []flux.Envelope{
		makeEnvelope(stream, 2, "second"),
	}

	err := store.Append(ctx, stream, 0, conflictingEvents)
	if err == nil {
		t.Fatalf("expected concurrency error, got nil")
	}
	if !errors.Is(err, flux.ErrConcurrency) {
		t.Fatalf("expected ErrConcurrency, got %v", err)
	}
}

func TestEventStore_StreamPreserved(t *testing.T) {
	t.Parallel()

	store := eventstore.New()
	ctx := context.Background()
	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:test:prod:items:123:item:stream-test"),
	}

	rawEnv := flux.Envelope{
		Event: dummyEvent{Value: "val"},
	}

	if err := store.Append(ctx, stream, 0, []flux.Envelope{rawEnv}); err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	iter, err := store.Read(ctx, stream, 0)
	if err != nil {
		t.Fatalf("failed to read stream: %v", err)
	}

	var found bool
	for env, err := range iter {
		if err != nil {
			t.Fatalf("unexpected iteration error: %v", err)
		}
		found = true
		if env.Stream.Identifier != stream.Identifier {
			t.Errorf("expected Stream identifier %v, got %v", stream.Identifier, env.Stream.Identifier)
		}
	}
	if !found {
		t.Fatal("expected at least one event in stream")
	}
}

func TestEventStore_ContextCancellation(t *testing.T) {
	t.Parallel()

	store := eventstore.New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:test:prod:items:123:item:cancel-test"),
	}

	err := store.Append(ctx, stream, 0, []flux.Envelope{{Event: dummyEvent{Value: "v"}}})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestEventStore_EmptyAppendNoOp(t *testing.T) {
	t.Parallel()

	store := eventstore.New()
	ctx := context.Background()
	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:test:prod:items:123:item:empty-test"),
	}

	// 1. Empty append on brand-new stream with expectedRevision 0 should succeed
	if err := store.Append(ctx, stream, 0, nil); err != nil {
		t.Fatalf("expected nil on empty append (nil), got %v", err)
	}
	if err := store.Append(ctx, stream, 0, []flux.Envelope{}); err != nil {
		t.Fatalf("expected nil on empty append (empty slice), got %v", err)
	}

	// 2. Append an event to advance revision to 1
	err := store.Append(ctx, stream, 0, []flux.Envelope{{Event: dummyEvent{Value: "v1"}}})
	if err != nil {
		t.Fatalf("failed to append event: %v", err)
	}

	// 3. Empty append on stream even with mismatching expectedRevision should be a no-op
	if err := store.Append(ctx, stream, 999, nil); err != nil {
		t.Errorf("expected nil on empty append even with mismatching expectedRevision, got %v", err)
	}
	if err := store.Append(ctx, stream, 999, []flux.Envelope{}); err != nil {
		t.Errorf("expected nil on empty append with empty slice even with mismatching expectedRevision, got %v", err)
	}
}

func TestEventStore_Find(t *testing.T) {
	t.Parallel()

	store := eventstore.New()
	ctx := context.Background()

	streamA := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:test:prod:items:123:item:stream-a"),
	}
	streamB := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:test:prod:items:123:item:stream-b"),
	}

	evtID1 := flux.MustParseIdentifier("urn:test:prod:items:123:event:evt1")
	evtID2 := flux.MustParseIdentifier("urn:test:prod:items:123:event:evt2")
	evtID3 := flux.MustParseIdentifier("urn:test:prod:items:123:event:evt3")
	unknownID := flux.MustParseIdentifier("urn:test:prod:items:123:event:unknown")

	eventsA := []flux.Envelope{
		{
			Identifier: evtID1,
			Event:      dummyEvent{Value: "first"},
			Metadata:   map[string]string{"key": "val1"},
		},
		{
			Identifier: evtID2,
			Event:      dummyEvent{Value: "second"},
			Metadata:   map[string]string{"key": "val2"},
		},
	}
	eventsB := []flux.Envelope{
		{
			Identifier: evtID3,
			Event:      dummyEvent{Value: "third"},
			Metadata:   map[string]string{"key": "val3"},
		},
	}

	if err := store.Append(ctx, streamA, 0, eventsA); err != nil {
		t.Fatalf("failed to append stream A: %v", err)
	}
	if err := store.Append(ctx, streamB, 0, eventsB); err != nil {
		t.Fatalf("failed to append stream B: %v", err)
	}

	// 1. Success hit on stream A
	env1, err := store.Find(ctx, streamA, evtID1)
	if err != nil {
		t.Fatalf("expected to find evtID1 in streamA, got err: %v", err)
	}
	if env1.Identifier != evtID1 {
		t.Errorf("expected identifier %s, got %s", evtID1, env1.Identifier)
	}
	if env1.Revision != 1 {
		t.Errorf("expected revision 1, got %d", env1.Revision)
	}
	if env1.Position != 1 {
		t.Errorf("expected position 1, got %d", env1.Position)
	}
	if de, ok := env1.Event.(dummyEvent); !ok || de.Value != "first" {
		t.Errorf("expected dummyEvent with 'first', got %v", env1.Event)
	}
	if env1.Metadata["key"] != "val1" {
		t.Errorf("expected metadata 'val1', got %q", env1.Metadata["key"])
	}

	// Verify metadata clone isolation
	env1.Metadata["mutated"] = "true"
	env1Refetch, err := store.Find(ctx, streamA, evtID1)
	if err != nil {
		t.Fatalf("expected refetch to succeed: %v", err)
	}
	if _, exists := env1Refetch.Metadata["mutated"]; exists {
		t.Errorf("expected Metadata to be isolated copy, but found mutated key in subsequent Find")
	}

	// 2. Success hit for second event on stream A
	env2, err := store.Find(ctx, streamA, evtID2)
	if err != nil {
		t.Fatalf("expected to find evtID2 in streamA, got err: %v", err)
	}
	if env2.Revision != 2 {
		t.Errorf("expected revision 2, got %d", env2.Revision)
	}
	if env2.Position != 2 {
		t.Errorf("expected position 2, got %d", env2.Position)
	}

	// 3. Unknown ID on stream A -> ErrEventNotFound
	_, err = store.Find(ctx, streamA, unknownID)
	if !errors.Is(err, flux.ErrEventNotFound) {
		t.Errorf("expected ErrEventNotFound for unknown event, got %v", err)
	}

	// 4. Valid ID from stream B searched on stream A -> ErrEventNotFound
	_, err = store.Find(ctx, streamA, evtID3)
	if !errors.Is(err, flux.ErrEventNotFound) {
		t.Errorf("expected ErrEventNotFound for event from wrong stream, got %v", err)
	}

	// 5. Nonexistent stream -> ErrEventNotFound
	nonexistentStream := flux.Stream{Identifier: flux.MustParseIdentifier("urn:test:prod:items:123:item:nonexistent")}
	_, err = store.Find(ctx, nonexistentStream, evtID1)
	if !errors.Is(err, flux.ErrEventNotFound) {
		t.Errorf("expected ErrEventNotFound for nonexistent stream, got %v", err)
	}

	// 6. Empty eventID -> ErrEventNotFound
	_, err = store.Find(ctx, streamA, flux.Identifier{})
	if !errors.Is(err, flux.ErrEventNotFound) {
		t.Errorf("expected ErrEventNotFound for empty eventID, got %v", err)
	}

	// 7. Context cancelled
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = store.Find(canceledCtx, streamA, evtID1)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}
