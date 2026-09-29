# Projections (Read Models)

Projections are optimized views of your data designed specifically for querying. Because events are your absolute source of truth, you can project them into any database (PostgreSQL, Redis, Elasticsearch) in whatever shape the frontend requires.

This is the "Q" (Query) side of Command Query Responsibility Segregation (CQRS).

## The Concept

While an Aggregate (Write Model) is optimized to protect business invariants, a Projection (Read Model) is optimized for lightning-fast reads. 

For example, when an `OrderPlaced` event occurs, a projector might:
1. Insert a row into a PostgreSQL `orders` table.
2. Increment a Redis counter for `daily_sales`.
3. Index the order details in Elasticsearch for text search.

When the frontend asks for data, your API simply queries these Read Models directly.

## Building a Synchronous Projector

For simple, low-volume applications, you can wire a projector directly to the `flux.EventBus`. This means the projection updates synchronously in memory right after the event occurs.

```go
package cataloglist

import (
	"context"
	"database/sql"
	"github.com/wotek/flux"
	"github.com/wotek/flux/event"
	catalogEvents "e-commerce/internal/catalog/events"
)

// Projector holds the database connection
type Projector struct {
	db *sql.DB
}

// Handle routes events to specific SQL mutations
func (p *Projector) Handle(ctx event.Context, e flux.Event) error {
	switch ev := e.(type) {
		
	case catalogEvents.ProductCreated:
		_, err := p.db.ExecContext(ctx, 
			"INSERT INTO catalog_list (id, name, price) VALUES (?, ?, ?)", 
			ctx.Stream().Identifier.String(), ev.Name, ev.Price,
		)
		return err
		
	case catalogEvents.PriceUpdated:
		_, err := p.db.ExecContext(ctx, 
			"UPDATE catalog_list SET price = ? WHERE id = ?", 
			ev.NewPrice, ctx.Stream().Identifier.String(),
		)
		return err
	}
	
	return nil
}

// Bind to the EventBus in main.go
// eventBus.RegisterGlobal(projector.Handle)
```

## Native Asynchronous Projections

For services running without an external orchestrator, `flux` provides an in-process, asynchronous projector engine in the `projection` package. The `Projector` tails the `EventStore` in the background, invokes registered event handlers, and maintains position checkpoints through a `projection.Store`.

```go
package main

import (
	"context"

	"github.com/wotek/flux"
	checkpointstore "github.com/wotek/flux/checkpoint/store"
	"github.com/wotek/flux/projection"
	projstore "github.com/wotek/flux/projection/store"
)

type CatalogItem struct {
	ID    string
	Name  string
	Price int
}

func main() {
	ctx := context.Background()
	consumerID := flux.MustParseIdentifier("urn:ecommerce:prod:catalog:global:projector:catalog-list")

	// 1. Configure checkpoint store (in-memory for tests/prototypes, MySQL/Redis for production)
	checkpointStore := checkpointstore.New()

	// 2. Instantiate projection store composed with checkpoint persistence
	projStore := projstore.New(projstore.WithCheckpointStore(checkpointStore))

	// 3. Application read-model storage
	catalogItems := make(map[string]CatalogItem)

	// 4. Create and wire the projector
	projector := projection.NewProjector(consumerID, eventStore, projStore)

	projection.RegisterHandler(projector, func(ctx projection.Context, ev ProductCreated) error {
		catalogItems["product:"+ev.ProductID] = CatalogItem{
			ID:    ev.ProductID,
			Name:  ev.Name,
			Price: ev.Price,
		}
		return nil
	})

	// 5. Start background tailing
	go func() {
		if err := projector.Start(ctx); err != nil {
			panic(err)
		}
	}()
}
```

::: tip In-Memory Projection Store Mutex
When composing `projection/store.New(WithCheckpointStore(...))`, `Update` synchronizes execution using a process-local mutex across `mutate` and `SetPosition`; injecting remote durable checkpoint stores holds this mutex across network I/O and does not provide same-database transactional atomicity.
:::

### At-Least-Once Delivery & Idempotency

The native `Projector` processes stream envelopes sequentially. When a handler fails:
- The projection stops immediately and returns the error (fail-fast).
- The checkpoint position is **not** advanced.
- When the projector restarts, it queries `GetPosition` from the `projection.Store` and resumes stream tailing from the last committed checkpoint.

Because replaying from a checkpoint may re-deliver envelopes that were partially handled before a crash, handlers must be designed to be strictly idempotent.

### Durable Transactional Projections (MySQL)

When read-model tables and checkpoints reside in the same MySQL database, [`projection/store/mysql`](/backends/mysql#projection-store-transactional-update) guarantees that read-model mutations and checkpoint position updates commit atomically in a single database transaction. If a process crashes before commit, the transaction rolls back cleanly without advancing the cursor. On recovery, the uncommitted envelope is replayed, so handlers must remain strictly idempotent.

## Choosing a Projection Runtime: Native vs. Temporal

| Runtime | Position / Cursor Tracking | State Storage | Best Used For |
|---------|----------------------------|---------------|---------------|
| **Native `Projector`** | `checkpoint.Store` (MySQL, Redis, in-memory) | User-defined read store via `projection.Store` | Embedded services, microservices without Temporal infrastructure, high-throughput linear tailing |
| **Temporal Workflow** | Workflow execution history + `ContinueAsNew` | External database (Postgres, Elasticsearch, Redis) | Complex workflows, distributed replays, robust built-in retry policies, activities with long timeouts |

::: danger Anti-Pattern: Dual-Cursor Split Brain
Never mix the native `checkpoint.Store` and Temporal cursor tracking for the same logical consumer or read model. Temporal workflows track stream position deterministically in their execution history (`lastRevision`). Temporal activities must **never** call `SetPosition` on a flux `checkpoint.Store` for that same consumer, as external checkpoint mutations will conflict with Temporal history replays.
:::

## Continuous Tailing Projections with Temporal

For teams operating Temporal, projections can be implemented as long-running, continuous workflows.

### 1. The Tailing Workflow
Instead of listening to the live `EventBus`, the projection workflow asks the database for batches of historical events.

```go
func CatalogListProjectionWorkflow(ctx workflow.Context, lastRevision int) error {
	for {
		var batch []flux.Envelope
		
		// 1. Fetch the next batch of events from the database
		err := workflow.ExecuteActivity(ctx, FetchEventsActivity, lastRevision).Get(ctx, &batch)
		if err != nil {
			return err
		}
		
		// 2. Project them sequentially
		for _, env := range batch {
			err := workflow.ExecuteActivity(ctx, UpdateSQLActivity, env).Get(ctx, nil)
			if err != nil {
				return err // Temporal automatically retries on database failures!
			}
			lastRevision = env.GlobalPosition
		}
		
		// 3. Sleep until the live EventBus wakes us up
		if len(batch) == 0 {
			// (See the Workflows guide for the WakeUp signal implementation)
			WaitForWakeUpSignal(ctx)
		}
	}
}
```

### 2. The SQL Activity
The Activity does the exact same switch-statement logic as the synchronous projector, but it is now fully durable and retryable!

```go
func UpdateSQLActivity(ctx context.Context, env flux.Envelope) error {
	db := getDatabaseConnection() // Your dependency injection here
	
	switch ev := env.Event.(type) {
	case *catalogEvents.ProductCreated:
		_, err := db.ExecContext(ctx, 
			"INSERT INTO catalog_list (id, name) VALUES (?, ?)", 
			env.Stream.Identifier.String(), ev.Name,
		)
		return err
	}
	return nil
}
```

## Rebuilding Read Models

The absolute greatest superpower of Event Sourcing is **Replayability**. 

If you decide your frontend needs a `total_sales` column added to the `catalog_list` table, you don't have to write a massive, complex SQL migration script. 

You simply:
1. Add the column to your table schema.
2. Update your `UpdateSQLActivity` to populate that column.
3. **Start a brand new Temporal Projection Workflow with `lastRevision = 0`.**

The workflow will instantly rip through your entire `EventStore` history from the dawn of time, completely rebuilding the Read Model from scratch with 100% accuracy in a matter of seconds or minutes!
