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

## Continuous Tailing Projections (Enterprise)

Synchronous projections are easy, but they have a fatal flaw: if your database is down, the projector errors out and the event is lost from the read model. Furthermore, if you ever need to completely rebuild your Read Model from scratch (e.g., adding a new SQL column), synchronous projectors can't do it.

For production systems, **Projections should be continuous background workers that "tail" the Event Store.**

We highly recommend using a Temporal Workflow for this (as outlined in the [Workflows guide](/guide/workflows)). 

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
