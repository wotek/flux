# 6. Building Projections (Read Models)

Our Write Model (Aggregates) is structurally perfect for enforcing business rules. However, the `EventStore` is practically useless for querying. You cannot run a query like `SELECT * FROM Orders WHERE CustomerID = '123'` against an append-only event log.

To solve this, we use the "Query" side of CQRS: **Projections**.

## What is a Projection?

A Projection listens to the continuous stream of Domain Events and "projects" those facts into a secondary database optimized for reading. This could be a relational SQL table, a MongoDB document collection, or even a Redis cache.

When the UI requests data, it queries this read model directly, completely bypassing the EventStore and the Aggregates.

## 1. Defining the Projector

Let's build a projection that maintains a searchable SQL table of Products for our Catalog domain.

```go
package projections

import (
	"database/sql"
	"github.com/wotek/flux/event"
	"e-commerce/internal/catalog/events"
)

// ProductViewProjector updates a relational SQL table whenever product events occur.
type ProductViewProjector struct {
	DB *sql.DB
}

// HandleProductCreated inserts a new row into the read model.
func (p *ProductViewProjector) HandleProductCreated(ctx event.Context, e events.ProductCreated) error {
	// The event.Context provides the exact Stream ID!
	productID := ctx.Stream().Identifier.String()

	query := `INSERT INTO product_view (id, name, price) VALUES (?, ?, ?)`
	_, err := p.DB.ExecContext(ctx, query, productID, e.Name, e.Price)
	return err
}

// HandlePriceUpdated updates an existing row.
func (p *ProductViewProjector) HandlePriceUpdated(ctx event.Context, e events.PriceUpdated) error {
	productID := ctx.Stream().Identifier.String()

	query := `UPDATE product_view SET price = ? WHERE id = ?`
	_, err := p.DB.ExecContext(ctx, query, e.NewPrice, productID)
	return err
}
```

## 2. Idempotency & The Event Context

One of the most critical aspects of Projections is that they must be **idempotent**. If the system crashes and replays historical events to catch up, your projector might receive the same `ProductCreated` event twice.

Notice how our handlers take an `event.Context` instead of a standard `context.Context`? The `event.Context` provides essential metadata about the Envelope:

```go
revision := ctx.Revision() // e.g., 1
globalPos := ctx.Position() // e.g., 4205
```

You can use the `GlobalPosition` to track exactly which events your projector has already processed by saving it alongside your read model data in the same SQL transaction. If you receive an event with a global position less than or equal to what you've saved, you can safely skip it!

## 3. Wiring the Event Bus

Finally, we register our projector methods to the global **Event Bus**. Unlike the Command Bus (which is 1:1), the Event Bus is **1:N** (Pub/Sub). Multiple projectors can listen to the exact same event.

```go
import "github.com/wotek/flux/event"

func main() {
	eventBus := event.New()
	
	projector := &projections.ProductViewProjector{DB: sqlDB}

	// Registering the handlers
	event.Register(eventBus, projector.HandleProductCreated)
	event.Register(eventBus, projector.HandlePriceUpdated)
}
```

Now, whenever an Aggregate successfully saves to the `EventStore`, the framework will publish the resulting events to the `EventBus`, and our `product_view` SQL table will instantly update!

In our final chapter, we will tackle the hardest problem in distributed systems: long-running cross-domain workflows.
