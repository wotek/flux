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
| --- | --- | --- | --- |
| **Native `Projector`** | `checkpoint.Store` (MySQL, Redis, in-memory) | User-defined read store via `projection.Store` | Embedded services, high-throughput linear tailing, same-DB transactional MySQL updates |
| **Temporal Workflow** | Workflow history + `ContinueAsNew` (`lastPosition`) | External database (Postgres, Elasticsearch, Redis, …) | Durable retries, long rebuilds, shared Temporal ops with workflows |

Pick **one** runtime per logical read model. Native `Projector` + `checkpoint.Store` is first-class for embedded services. Temporal hybrid tailing is **optional** when you already run Temporal or need durable rebuilds. See [Workflows & Temporal](/guide/workflows) for shared conventions (workflow IDs, WakeUp signals, DTO payloads).

::: danger Anti-pattern: dual-cursor split brain
Never mix flux `checkpoint.Store` and Temporal cursor tracking for the same logical consumer. Temporal activities must **never** call `SetPosition` on a flux checkpoint for that consumer.
:::

## Continuous Tailing Projections with Temporal

Run a Temporal workflow that batches from the event store, applies activities, waits on `WakeUpSignal` when caught up, and `ContinueAsNew`s to bound history. Pass **serializable envelope DTOs** (IDs + payload bytes / typed structs)—not `flux.Envelope` with an interface `Event`—across the Temporal boundary.

```go
func CatalogListProjectionWorkflow(ctx workflow.Context, lastPosition uint64) error {
	for {
		var batch []EnvelopeDTO
		err := workflow.ExecuteActivity(ctx, FetchEventsActivity, lastPosition).Get(ctx, &batch)
		if err != nil {
			return err
		}
		for _, env := range batch {
			err := workflow.ExecuteActivity(ctx, UpdateSQLActivity, env).Get(ctx, nil)
			if err != nil {
				return err
			}
			lastPosition = env.Position
		}
		if len(batch) == 0 {
			WaitForWakeUpSignal(ctx) // see Workflows guide
		}
	}
}
```

### SQL Activity

```go
func UpdateSQLActivity(ctx context.Context, env EnvelopeDTO) error {
	// Decode env into a concrete event via your type registry / codec, then upsert the read model.
	return nil
}
```

## Rebuilding Read Models

To rebuild a read model:

1. Add schema columns as needed.
2. Update the projection activity / handler.
3. Start a **new** Temporal projection workflow with `lastPosition = 0` (new workflow ID), **or** reset / use a new native projector consumer ID.

Do not dual-write a flux checkpoint while Temporal owns the cursor.
