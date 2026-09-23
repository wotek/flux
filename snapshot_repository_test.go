package flux_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
	"github.com/oklog/ulid/v2"

	"github.com/wotek/flux"
	eventstore "github.com/wotek/flux/event/store"
)

// inMemorySnapshotStore is a thread-safe in-memory implementation of flux.SnapshotStore for tests.
type inMemorySnapshotStore[S any] struct {
	mu        sync.RWMutex
	snapshots map[string]flux.Snapshot[S]
	loadErr   error
	saveErr   error
}

func newInMemorySnapshotStore[S any]() *inMemorySnapshotStore[S] {
	return &inMemorySnapshotStore[S]{
		snapshots: make(map[string]flux.Snapshot[S]),
	}
}

func (s *inMemorySnapshotStore[S]) Load(_ context.Context, stream flux.Stream) (flux.Snapshot[S], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.loadErr != nil {
		return flux.Snapshot[S]{}, s.loadErr
	}

	snap, ok := s.snapshots[stream.Identifier.String()]
	if !ok {
		return flux.Snapshot[S]{}, errors.New("snapshot not found")
	}
	return snap, nil
}

func (s *inMemorySnapshotStore[S]) Save(_ context.Context, stream flux.Stream, snap flux.Snapshot[S]) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.saveErr != nil {
		return s.saveErr
	}

	s.snapshots[stream.Identifier.String()] = snap
	return nil
}

// Mock Aggregate and Event for testing snapshots.

type itemEvent interface {
	flux.Event
	isItemEvent()
}

type itemAddedEvent struct {
	Item string `json:"item"`
}

func (e itemAddedEvent) Name() string { return "ItemAdded" }
func (e itemAddedEvent) isItemEvent() {}

// itemState is the Memento DTO for itemAggregate.
type itemState struct {
	Items []string `json:"items"`
}

type itemAggregate struct {
	flux.AggregateRoot[itemEvent]
	items []string
}

func newItemAggregate(stream flux.Stream) *itemAggregate {
	a := &itemAggregate{items: make([]string, 0)}
	a.AggregateRoot = flux.NewAggregateRoot[itemEvent](stream, flux.NewChangeset[itemEvent](), a.apply)
	return a
}

func (a *itemAggregate) New(stream flux.Stream) *itemAggregate {
	return newItemAggregate(stream)
}

func (a *itemAggregate) apply(e itemEvent) error {
	switch evt := e.(type) {
	case itemAddedEvent:
		a.items = append(a.items, evt.Item)
	}
	return nil
}

func (a *itemAggregate) AddItem(item string) {
	evt := itemAddedEvent{Item: item}
	a.Changeset().Record(evt)
	_ = a.apply(evt)
}

func (a *itemAggregate) Snapshot() itemState {
	itemsCopy := make([]string, len(a.items))
	copy(itemsCopy, a.items)
	return itemState{Items: itemsCopy}
}

func (a *itemAggregate) With(state itemState) {
	a.items = make([]string, len(state.Items))
	copy(a.items, state.Items)
}

func newTestContext() flux.Context {
	return flux.NewContext(context.Background(), flux.Actor{}, flux.Identifier{}, flux.Identifier{})
}

func newStream(id string) flux.Stream {
	return flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:test:prod:items:123:item:" + id),
	}
}

// TestSnapshotRepository_Threshold verifies that Save triggers a snapshot when the schedule condition is met.
func TestSnapshotRepository_Threshold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		threshold     uint64
		eventsToAdd   int
		wantSnapshot  bool
		wantRevision  uint64
		wantItemCount int
	}{
		{
			name:          "below threshold",
			threshold:     3,
			eventsToAdd:   2,
			wantSnapshot:  false,
			wantRevision:  0,
			wantItemCount: 0,
		},
		{
			name:          "exactly at threshold",
			threshold:     3,
			eventsToAdd:   3,
			wantSnapshot:  true,
			wantRevision:  3,
			wantItemCount: 3,
		},
		{
			name:          "above threshold before second interval",
			threshold:     3,
			eventsToAdd:   4,
			wantSnapshot:  false,
			wantRevision:  0,
			wantItemCount: 0,
		},
		{
			name:          "at second interval threshold",
			threshold:     3,
			eventsToAdd:   6,
			wantSnapshot:  true,
			wantRevision:  6,
			wantItemCount: 6,
		},
		{
			name:          "threshold of 1 triggers on single event",
			threshold:     1,
			eventsToAdd:   1,
			wantSnapshot:  true,
			wantRevision:  1,
			wantItemCount: 1,
		},
		{
			name:          "threshold of 0 never triggers",
			threshold:     0,
			eventsToAdd:   5,
			wantSnapshot:  false,
			wantRevision:  0,
			wantItemCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := newTestContext()
			es := eventstore.New()
			snapStore := newInMemorySnapshotStore[itemState]()
			baseRepo := flux.NewAggregateRepository[*itemAggregate, itemEvent](es)
			schedule := flux.Every[*itemAggregate, itemEvent](tt.threshold)
			snapRepo := flux.NewSnapshotRepository(baseRepo, snapStore, schedule, es)

			stream := newStream(tt.name)
			agg := newItemAggregate(stream)

			for i := 1; i <= tt.eventsToAdd; i++ {
				agg.AddItem(fmt.Sprintf("item-%d", i))
			}

			if err := snapRepo.Save(ctx, agg); err != nil {
				t.Fatalf("failed to save aggregate: %v", err)
			}

			snap, err := snapStore.Load(ctx, stream)
			if tt.wantSnapshot {
				if err != nil {
					t.Fatalf("expected snapshot to be saved, got error: %v", err)
				}
				if snap.Revision != tt.wantRevision {
					t.Errorf("expected snapshot revision %d, got %d", tt.wantRevision, snap.Revision)
				}
				if len(snap.State.Items) != tt.wantItemCount {
					t.Errorf("expected %d items in snapshot state, got %d", tt.wantItemCount, len(snap.State.Items))
				}
			} else {
				if err == nil {
					t.Errorf("expected no snapshot to be saved, but found one with revision %d", snap.Revision)
				}
			}
		})
	}
}

// TestSnapshotRepository_CatchUp verifies that Load correctly hydrates from a snapshot and replays trailing events.
func TestSnapshotRepository_CatchUp(t *testing.T) {
	t.Parallel()

	ctx := newTestContext()
	es := eventstore.New()
	snapStore := newInMemorySnapshotStore[itemState]()
	baseRepo := flux.NewAggregateRepository[*itemAggregate, itemEvent](es)
	schedule := flux.Every[*itemAggregate, itemEvent](3)
	snapRepo := flux.NewSnapshotRepository(baseRepo, snapStore, schedule, es)

	stream := newStream("catchup-test")
	agg := newItemAggregate(stream)

	// Add 3 items and save (triggering snapshot at revision 3)
	agg.AddItem("item-1")
	agg.AddItem("item-2")
	agg.AddItem("item-3")

	if err := snapRepo.Save(ctx, agg); err != nil {
		t.Fatalf("failed to save initial aggregate: %v", err)
	}

	snap, err := snapStore.Load(ctx, stream)
	if err != nil {
		t.Fatalf("expected snapshot at revision 3: %v", err)
	}
	if snap.Revision != 3 {
		t.Fatalf("expected snapshot revision 3, got %d", snap.Revision)
	}

	// Now simulate trailing events appended directly to EventStore after snapshot was taken
	trailingEnvelopes := []flux.Envelope{
		{
			Identifier: flux.NewIdentifier("", "", "stream", "", "event", ulid.Make().String(), ""),
			Stream:     stream,
			Revision:   4,
			Event:      itemAddedEvent{Item: "item-4"},
			CreatedAt:  time.Now(),
		},
		{
			Identifier: flux.NewIdentifier("", "", "stream", "", "event", ulid.Make().String(), ""),
			Stream:     stream,
			Revision:   5,
			Event:      itemAddedEvent{Item: "item-5"},
			CreatedAt:  time.Now(),
		},
	}
	if err := es.Append(ctx, stream, 3, trailingEnvelopes); err != nil {
		t.Fatalf("failed to append trailing events: %v", err)
	}

	// Load aggregate through SnapshotRepository
	loaded, err := snapRepo.Load(ctx, stream)
	if err != nil {
		t.Fatalf("failed to load aggregate: %v", err)
	}

	// Verify revision caught up to 5
	if loaded.Revision() != 5 {
		t.Errorf("expected aggregate revision 5, got %d", loaded.Revision())
	}

	// Verify all 5 items are present in order
	expectedItems := []string{"item-1", "item-2", "item-3", "item-4", "item-5"}
	if !slices.Equal(loaded.items, expectedItems) {
		t.Errorf("expected items %v, got %v", expectedItems, loaded.items)
	}

	// Verify uncommitted changeset is empty
	if loaded.Changeset().HasChanges() {
		t.Errorf("expected empty changeset on loaded aggregate")
	}
}

// TestSnapshotRepository_Serialization verifies that the Memento DTO accurately preserves state
// through JSON serialization without requiring the aggregate itself to implement marshaling interfaces.
func TestSnapshotRepository_Serialization(t *testing.T) {
	t.Parallel()

	stream := newStream("serialization-test")
	agg := newItemAggregate(stream)
	agg.AddItem("apple")
	agg.AddItem("banana")
	agg.AddItem("cherry")

	// Extract snapshot Memento DTO
	state := agg.Snapshot()
	snapshot := flux.Snapshot[itemState]{
		State:    state,
		Revision: 3,
	}

	// Serialize snapshot to JSON
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("failed to marshal snapshot: %v", err)
	}

	// Deserialize snapshot from JSON
	var restoredSnapshot flux.Snapshot[itemState]
	if err := json.Unmarshal(data, &restoredSnapshot); err != nil {
		t.Fatalf("failed to unmarshal snapshot: %v", err)
	}

	if restoredSnapshot.Revision != 3 {
		t.Errorf("expected restored revision 3, got %d", restoredSnapshot.Revision)
	}

	expectedItems := []string{"apple", "banana", "cherry"}
	if !slices.Equal(restoredSnapshot.State.Items, expectedItems) {
		t.Errorf("expected restored items %v, got %v", expectedItems, restoredSnapshot.State.Items)
	}

	// Rehydrate a new aggregate directly using the deserialized Memento
	restoredAgg := newItemAggregate(stream)
	restoredAgg.With(restoredSnapshot.State)
	if !slices.Equal(restoredAgg.items, expectedItems) {
		t.Errorf("expected restored aggregate items %v, got %v", expectedItems, restoredAgg.items)
	}

	// Verify end-to-end hydration through SnapshotRepository.Load using the deserialized snapshot
	ctx := newTestContext()
	es := eventstore.New()
	snapStore := newInMemorySnapshotStore[itemState]()
	if err := snapStore.Save(ctx, stream, restoredSnapshot); err != nil {
		t.Fatalf("failed to save restored snapshot: %v", err)
	}
	baseRepo := flux.NewAggregateRepository[*itemAggregate, itemEvent](es)
	schedule := flux.Every[*itemAggregate, itemEvent](10)
	snapRepo := flux.NewSnapshotRepository(baseRepo, snapStore, schedule, es)

	loaded, err := snapRepo.Load(ctx, stream)
	if err != nil {
		t.Fatalf("failed to load aggregate from deserialized snapshot: %v", err)
	}
	if loaded.Revision() != 3 {
		t.Errorf("expected loaded aggregate revision 3, got %d", loaded.Revision())
	}
	if !slices.Equal(loaded.items, expectedItems) {
		t.Errorf("expected loaded aggregate items %v, got %v", expectedItems, loaded.items)
	}
}

// TestSnapshotRepository_FallbackToEventSource verifies that Load successfully hydrates from events
// when no snapshot is present.
func TestSnapshotRepository_FallbackToEventSource(t *testing.T) {
	t.Parallel()

	ctx := newTestContext()
	es := eventstore.New()
	snapStore := newInMemorySnapshotStore[itemState]()
	baseRepo := flux.NewAggregateRepository[*itemAggregate, itemEvent](es)
	schedule := flux.Every[*itemAggregate, itemEvent](10) // high threshold, no snapshot taken
	snapRepo := flux.NewSnapshotRepository(baseRepo, snapStore, schedule, es)

	stream := newStream("fallback-test")
	agg := newItemAggregate(stream)
	agg.AddItem("item-1")
	agg.AddItem("item-2")

	if err := snapRepo.Save(ctx, agg); err != nil {
		t.Fatalf("failed to save aggregate: %v", err)
	}

	// Verify no snapshot was saved
	if _, err := snapStore.Load(ctx, stream); err == nil {
		t.Fatalf("expected no snapshot in store")
	}

	// Load aggregate; should replay from event store
	loaded, err := snapRepo.Load(ctx, stream)
	if err != nil {
		t.Fatalf("failed to load aggregate: %v", err)
	}

	if loaded.Revision() != 2 {
		t.Errorf("expected revision 2, got %d", loaded.Revision())
	}
	expectedItems := []string{"item-1", "item-2"}
	if !slices.Equal(loaded.items, expectedItems) {
		t.Errorf("expected items %v, got %v", expectedItems, loaded.items)
	}
}

// TestSnapshotRepository_NotFound verifies that Load returns an error when neither snapshot nor events exist.
func TestSnapshotRepository_NotFound(t *testing.T) {
	t.Parallel()

	ctx := newTestContext()
	es := eventstore.New()
	snapStore := newInMemorySnapshotStore[itemState]()
	baseRepo := flux.NewAggregateRepository[*itemAggregate, itemEvent](es)
	schedule := flux.Every[*itemAggregate, itemEvent](1)
	snapRepo := flux.NewSnapshotRepository(baseRepo, snapStore, schedule, es)

	stream := newStream("non-existent")
	_, err := snapRepo.Load(ctx, stream)
	if err == nil {
		t.Fatalf("expected error loading non-existent aggregate, got nil")
	}
	if !errors.Is(err, flux.ErrAggregateNotFound) {
		t.Fatalf("expected ErrAggregateNotFound, got %v", err)
	}
}

// TestSnapshotRepository_CustomSchedule verifies that SnapshotScheduleFunc works as expected.
func TestSnapshotRepository_CustomSchedule(t *testing.T) {
	t.Parallel()

	ctx := newTestContext()
	es := eventstore.New()
	snapStore := newInMemorySnapshotStore[itemState]()
	baseRepo := flux.NewAggregateRepository[*itemAggregate, itemEvent](es)

	// Schedule that triggers whenever items contains "trigger"
	customSchedule := flux.SnapshotScheduleFunc[*itemAggregate](func(agg *itemAggregate) bool {
		return slices.Contains(agg.items, "trigger")
	})

	snapRepo := flux.NewSnapshotRepository(baseRepo, snapStore, customSchedule, es)

	stream := newStream("custom-schedule-test")
	agg := newItemAggregate(stream)
	agg.AddItem("regular-item")

	if err := snapRepo.Save(ctx, agg); err != nil {
		t.Fatalf("failed to save aggregate: %v", err)
	}

	if _, err := snapStore.Load(ctx, stream); err == nil {
		t.Errorf("expected no snapshot when trigger item not present")
	}

	agg.AddItem("trigger")
	if err := snapRepo.Save(ctx, agg); err != nil {
		t.Fatalf("failed to save aggregate with trigger: %v", err)
	}

	snap, err := snapStore.Load(ctx, stream)
	if err != nil {
		t.Fatalf("expected snapshot when trigger item is present: %v", err)
	}
	if snap.Revision != 2 {
		t.Errorf("expected revision 2, got %d", snap.Revision)
	}
}

// TestSnapshotRepository_StoreErrorHandling verifies error handling when SnapshotStore fails.
func TestSnapshotRepository_StoreErrorHandling(t *testing.T) {
	t.Parallel()

	ctx := newTestContext()
	es := eventstore.New()
	snapStore := newInMemorySnapshotStore[itemState]()
	snapStore.saveErr = errors.New("disk full")

	baseRepo := flux.NewAggregateRepository[*itemAggregate, itemEvent](es)
	schedule := flux.Every[*itemAggregate, itemEvent](1)
	snapRepo := flux.NewSnapshotRepository(baseRepo, snapStore, schedule, es)

	stream := newStream("store-error-test")
	agg := newItemAggregate(stream)
	agg.AddItem("item")

	err := snapRepo.Save(ctx, agg)
	if err == nil {
		t.Fatalf("expected error from Save when store fails, got nil")
	}
}
