# 6. Projections & Queries

Our Write Model (Aggregates) is structurally perfect for enforcing business rules. However, the `EventStore` is practically useless for querying. You cannot run a query like `SELECT * FROM Orders WHERE CustomerID = '123'` against an append-only event log.

To solve this, we complete the "Query" side of CQRS using **Projections** and the **Query Bus**.

## Part 1: Building the Projection

A Projection listens to the continuous stream of Domain Events and "projects" those facts into a secondary database optimized for reading. This could be a relational SQL table, a MongoDB document collection, or even a Redis cache.

When the UI requests data, it queries this read model directly, completely bypassing the EventStore and the Aggregates.

### 1. Defining the Projector

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
```

### 2. Idempotency & The Event Context

One of the most critical aspects of Projections is that they must be **idempotent**. If the system crashes and replays historical events to catch up, your projector might receive the same `ProductCreated` event twice.

You can use the `GlobalPosition` from the `event.Context` to track exactly which events your projector has already processed by saving it alongside your read model data in the same SQL transaction. If you receive an event with a global position less than or equal to what you've saved, you safely skip it.

```go
revision := ctx.Revision() // e.g., 1
globalPos := ctx.Position() // e.g., 4205
```

## Part 2: Fetching Data with the Query Bus

Now that our `product_view` table is being populated continuously in the background, we need a way for our API to fetch this data. We do this using the `Query Bus`.

### 1. Defining the Query

Just like Commands, Queries are plain Go structs that declare the data we want. We also define the expected result struct.

```go
package queries

// GetProduct is an intent to fetch a single product's view model.
type GetProduct struct {
	ProductID string
}

// ProductView is the read-model shape we will return to the frontend.
type ProductView struct {
	ID    string
	Name  string
	Price int
}
```

### 2. Writing the Query Handler

The Query Handler takes the Query struct, executes the highly optimized `SELECT` statement against our projection table, and returns the result.

```go
package handlers

import (
	"database/sql"
	"github.com/wotek/flux/query"
	"e-commerce/internal/catalog/queries"
)

// HandleGetProduct acts as the bridge between the Query Bus and our SQL projection.
func HandleGetProduct(db *sql.DB) func(ctx query.Context, q queries.GetProduct) (queries.ProductView, error) {
	return func(ctx query.Context, q queries.GetProduct) (queries.ProductView, error) {
		var view queries.ProductView
		
		stmt := `SELECT id, name, price FROM product_view WHERE id = ?`
		err := db.QueryRowContext(ctx, stmt, q.ProductID).Scan(&view.ID, &view.Name, &view.Price)
		
		if err != nil {
			return queries.ProductView{}, err
		}
		return view, nil
	}
}
```

### 3. Routing via the Query Bus

Just like the Command Bus, the Query Bus maps exactly **one** handler to a Query type. We register it in `main.go` using `flux`'s generic type inference.

```go
import "github.com/wotek/flux/query"

func main() {
	queryBus := query.New()

	// The generic RegisterHandler strictly ensures the inputs and outputs match!
	query.RegisterHandler(queryBus, handlers.HandleGetProduct(db))
}
```

### 4. Executing the Query in your API

When an HTTP GET request arrives, you ask the Query Bus for the data. You do not touch the `AggregateRepository` or the `EventStore`.

```go
func (api *Server) GetProductEndpoint(w http.ResponseWriter, r *http.Request) {
    productID := r.URL.Query().Get("id")

    // Construct the typed context
    ctx := query.NewContext(r.Context(), flux.MustParseIdentifier("urn:query:1"), flux.Actor{}, flux.Identifier{})

    // Execute the query. Notice how the return type is strongly typed to `queries.ProductView`!
    result, err := query.Ask(ctx, api.QueryBus, queries.GetProduct{ProductID: productID})
    if err != nil {
        http.Error(w, err.Error(), http.StatusNotFound)
        return
    }

    json.NewEncoder(w).Encode(result)
}
```

### Why use a Query Bus?

Why not just write `db.QueryRow` directly inside the HTTP handler? 

By using the `query.Bus`, you cleanly decouple your delivery mechanism (HTTP/gRPC) from your domain queries. More importantly, it allows you to use **Middlewares**. You can easily attach a Redis caching middleware to the Query Bus that intercepts `queries.GetProduct`, serves it from Redis instantly if available, and only executes the underlying SQL handler if there is a cache miss!

In our final chapter, we will tackle the hardest problem in distributed systems: long-running cross-domain workflows.
