# Snapshots Implementation Design

This document outlines the design and implementation plan for adding Aggregate Snapshotting to the `flux` framework leveraging Go's strong typing (Generics) and idiomatic patterns.

## 1. Package Structure & The Memento Pattern

To prevent polluting the Domain Aggregate API with infrastructure-level serialization concerns (like `json.Marshaler`), we employ the **Memento Pattern**. The aggregate exports and restores its state via a simple, strongly-typed DTO (the "Memento" or "State"), which the snapshot store can serialize automatically. 

All snapshot logic is seamlessly integrated directly into the core `flux` package.

```go
package flux

import (
	"context"
	"github.com/wotek/flux"
)

// Snapshotable defines how an aggregate safely exposes and restores its internal state.
type Snapshotable[S any] interface {
	Snapshot() S
	With(state S)
}

// Snapshot represents a captured point-in-time state of an Aggregate.
type Snapshot[S any] struct {
	State    S
	Revision uint64
}

// Store defines how snapshots are persisted and retrieved.
type SnapshotStore[S any] interface {
	Load(ctx context.Context, stream flux.Stream) (Snapshot[S], error)
	Save(ctx context.Context, stream flux.Stream, snap Snapshot[S]) error
}

// Schedule determines if an aggregate should be snapshotted based on its current state.
type SnapshotSchedule[A any] interface {
	Test(aggregate A) bool
}
```

## 2. Event Store Updates

To properly support snapshots, `flux.EventStore.Read` must be updated to accept a `fromRevision` parameter. Passing `0` acts as the default behavior to read from the beginning. 

```go
type EventStore interface {
	// Read retrieves events for a specific stream starting from the given position (revision).
	// Passing 0 reads the entire stream from the beginning.
	Read(ctx context.Context, stream flux.Stream, fromRevision uint64) (StreamIterator, error)
}
```

## 3. Decorator Repository (`flux.SnapshotRepository`)

We create a `flux.SnapshotRepository` that wraps the standard `flux.AggregateRepository`. We use interface composition to guarantee the aggregate supports both Event Sourcing and Snapshotting at compile time.

```go
// Aggregate constraint ensures the type passed is both a Flux Aggregate and Snapshotable.
type Aggregate[A any, E flux.Event, S any] interface {
	flux.Aggregate[A, E]
	Snapshotable[S]
}

type SnapshotRepository[A Aggregate[A, E, S], E flux.Event, S any] struct {
	base       *flux.AggregateRepository[A, E]
	store      SnapshotStore[S]
	schedule   SnapshotSchedule[A]
	eventStore flux.EventStore
}

func NewSnapshotRepository[A Aggregate[A, E, S], E flux.Event, S any](
	base *flux.AggregateRepository[A, E],
	store SnapshotStore[S],
	schedule SnapshotSchedule[A],
	eventStore flux.EventStore,
) *SnapshotRepository[A, E, S] {
	return &SnapshotRepository[A, E, S]{
		base:       base,
		store:      store,
		schedule:   schedule,
		eventStore: eventStore,
	}
}
```

## 4. The Write Path (Persist)

When `.Save()` is called, we delegate to the base repository, test the schedule, extract the Memento state, and persist the snapshot if true.

```go
func (r *SnapshotRepository[A, E, S]) Save(ctx flux.Context, aggregate A) error {
	if err := r.base.Save(ctx, aggregate); err != nil {
		return err
	}

	if !r.schedule.Test(aggregate) {
		return nil
	}

	snap := Snapshot[S]{
		State:    aggregate.Snapshot(),
		Revision: aggregate.Revision(),
	}

	stream := flux.Stream{Identifier: aggregate.Identifier()}
	return r.store.Save(ctx, stream, snap) 
}
```

## 5. The Read Path (Fetch & Refresh)

We rehydrate fully in a single `Load()` call to prevent returning a "stale" aggregate.

```go
func (r *SnapshotRepository[A, E, S]) Load(ctx flux.Context, stream flux.Stream) (A, error) {
	var agg A
	var startRevision uint64 = 0

	if snap, err := r.store.Load(ctx, stream); err == nil {
		// Rehydrate from snapshot
		agg = agg.New(stream) // Create empty shell
		agg.With(snap.State)
		
		// The framework type-asserts to its OWN unexported interface!
		if s, ok := any(agg).(interface{ setRevision(uint64) }); ok {
			s.setRevision(snap.Revision)
		}
		startRevision = snap.Revision
	} else {
		// Fallback to purely event-sourced
		var zero A
		agg = zero.New(stream)
	}

	// Catch up with trailing events
	events, err := r.eventStore.Read(ctx, stream, startRevision)
	if err != nil {
		return agg, fmt.Errorf("reading event stream: %w", err)
	}

	if err := agg.FromEvents(events); err != nil {
		return agg, fmt.Errorf("replaying events: %w", err)
	}

	return agg, nil
}
```

## 6. Flexible DDD Rigidity

Because the framework uses Generics (`[S any]`), the `Snapshotable[S any]` interface gives the developer absolute control over how much Domain-Driven Design rigidity they want to apply to each individual aggregate. 

### Pattern A: Large Aggregates (Opt-in Behavior/State Separation)
For complex aggregates (like `Order` or `Product`), it is highly recommended to isolate the pure domain data into an opt-in `types` package. The aggregate then becomes a pure behavior wrapper around the `types.Product` struct.

In this scenario, `S` is your pure types object.

```go
import "github.com/myorg/ecommerce/internal/catalog/types"

// ProductAggregate is just a behavior wrapper
type ProductAggregate struct {
	flux.AggregateRoot[ProductEvent]
	data types.Product
}

// Snapshot simply returns the wrapped pure data object
func (p *ProductAggregate) Snapshot() types.Product {
	return p.data
}

func (p *ProductAggregate) With(data types.Product) {
	p.data = data
	// Revision is handled safely by the framework!
}
```
*Benefit: True Behavior/State separation. The pure `types.Product` can be reused in projections or passed safely to other domain services.*

### Pattern B: Small Aggregates (Inline State)
For small or simple aggregates (like a simple `Counter` or `LikeButton`) where maintaining a separate `types` object is overkill, you can keep the fields inline on the aggregate. 

In this scenario, `S` is simply an unexported struct defined directly next to the aggregate.

```go
type CounterAggregate struct {
	flux.AggregateRoot[CounterEvent]
	clicks int
}

// Define a quick, unexported snapshot struct to act as 'S'
type counterSnapshot struct {
	Clicks int `json:"clicks"`
}

func (c *CounterAggregate) Snapshot() counterSnapshot {
	return counterSnapshot{Clicks: c.clicks}
}

func (c *CounterAggregate) With(data counterSnapshot) {
	c.clicks = data.Clicks
	// Revision is handled safely by the framework!
}
```
*Benefit: No unnecessary abstraction overhead. The `SnapshotRepository` serializes the `counterSnapshot` perfectly without the `CounterAggregate` needing to export its internal fields.*

## 7. Package Outline

### `flux/event_store.go`
- Modify `EventStore.Read(..., fromRevision uint64)`

### `flux/aggregate.go`
- Add unexported `func (a *AggregateRoot[E]) setRevision(rev uint64)`

### `flux/snapshot.go`
- `type Snapshotable[S any] interface`
- `type Snapshot[S any] struct`
- `type SnapshotStore[S any] interface`

### `flux/snapshot_schedule.go`
- `type SnapshotSchedule[A any] interface`
- `func Every[A flux.Aggregate[A, E], E flux.Event](n uint64) Schedule[A]`

### `flux/snapshot_repository.go`
- `type Aggregate[A any, E flux.Event, S any] interface`
- `type SnapshotRepository[A Aggregate[A, E, S], E flux.Event, S any] struct`
- `func NewSnapshotRepository(...) *SnapshotRepository`
- `func (r *SnapshotRepository) Load(...)`
- `func (r *SnapshotRepository) Save(...)`

## 8. Implementation Instructions (For Agents)

1. **Update EventStore:** Modify `flux/event_store.go` so `Read` accepts `fromRevision uint64`. Update `event/store/in_memory.go` to handle the `fromRevision` argument correctly (skipping `env.Revision <= fromRevision`).
2. **Expose Revision Setter:** In `flux/aggregate.go`, add `SetRevision(rev uint64)` to `AggregateRoot[E]`.
3. **Core Snapshot Types:** Create `flux/snapshot.go` and define `Snapshotable`, `Snapshot`, and `SnapshotStore`.
4. **Schedules:** Create `flux/snapshot_schedule.go` and implement `Every(n)`.
5. **Repository:** Create `flux/snapshot_repository.go` and implement the decorator logic for `Load` and `Save`.
6. **Testing & QA:** Create `flux/snapshot_repository_test.go` using a mock aggregate and an in-memory `SnapshotStore` (which you will write for the tests).
   - **Threshold Test:** Write standard table-driven, parallelized Go tests simulating an aggregate crossing the `snapshot.Every` threshold, ensuring `Save` correctly triggers the snapshot.
   - **Catch-up Test:** Write a test verifying that `Load` perfectly catches up an aggregate if new events were appended to the `EventStore` *after* the snapshot was taken.
   - **Serialization Test:** Add a specific test case where you run the snapshot struct `S` through `json.Marshal` and `json.Unmarshal` to verify that the Memento DTO accurately preserves state through standard serialization without requiring the aggregate to marshal itself.
   - Adhere strictly to the guidelines in `.rules`.
