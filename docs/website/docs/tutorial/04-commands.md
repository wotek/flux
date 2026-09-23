# 4. Commands & Command Bus

With our Aggregate fully tested and our Repository configured to save it, we need a way to route requests from our API (HTTP, gRPC, CLI) into our domain.

In `flux`, we enforce strict Command Query Responsibility Segregation (CQRS) using the **Command Bus**.

## Intents vs Facts

If an Event is a *Fact* (something that has already happened, like `ProductCreated`), a Command is an *Intent* (a request to do something, like `CreateProduct`). 

Commands can fail (e.g., validation fails, user unauthorized). Events cannot fail.

Let's define the commands for our Catalog domain. Create `internal/catalog/commands/product.go`:

```go
package commands

// CreateProduct is an intent to add a new product to the catalog.
type CreateProduct struct {
	ProductID string
	Name      string
	Price     int
}

// UpdateProductPrice is an intent to change the price of an existing product.
type UpdateProductPrice struct {
	ProductID string
	NewPrice  int
}
```

Notice that commands are just plain Go structs. They do not need to implement any interface!

## Writing the Handler

A Command Handler is the glue between your API request and your Domain Aggregate. It has three jobs:
1. Load the aggregate from the repository.
2. Call the aggregate's business logic method.
3. Save the aggregate back to the repository.

Because `flux` uses Go Generics, command handlers are strongly typed.

```go
package handlers

import (
	"fmt"
	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"e-commerce/internal/catalog/aggregates/product"
	"e-commerce/internal/catalog/commands"
)

// HandleCreateProduct loads the aggregate, creates it, and saves the events.
func HandleCreateProduct(repo *flux.AggregateRepository[*product.Product, flux.Event]) func(ctx command.Context, cmd commands.CreateProduct) error {
	return func(ctx command.Context, cmd commands.CreateProduct) error {
		// 1. Build the Stream Identifier
		streamID := flux.MustParseIdentifier(fmt.Sprintf("urn:catalog:product:%s", cmd.ProductID))
		stream := flux.Stream{Identifier: streamID}

		// 2. Load the Aggregate (or create new if it doesn't exist)
		prod, err := repo.Load(ctx, stream)
		if err != nil {
			// If it's a new product, instantiate it
			prod = product.New(stream)
		}

		// 3. Execute Business Logic
		if err := prod.Create(cmd.Name, cmd.Price); err != nil {
			return err // e.g., ErrInvalidPrice
		}

		// 4. Persist the new state
		return repo.Save(ctx, prod)
	}
}
```

## Routing via the Command Bus

Now, in your `cmd/server/main.go`, you initialize the Command Bus and register the handler.

The Command Bus guarantees a **1:1** routing ratio. If you dispatch a command that has no registered handler, or if multiple handlers are registered for the same command type, the bus will return an error immediately.

```go
import (
	"github.com/wotek/flux/command"
	"e-commerce/internal/catalog/commands"
	"e-commerce/internal/catalog/handlers"
)

func main() {
    // ... repository initialization from previous chapter ...

    // 1. Initialize the Bus
    cmdBus := command.New()

    // 2. Register the Handler
    // The framework uses generics to automatically map the commands.CreateProduct struct
    // to this specific handler function.
    command.Register(cmdBus, handlers.HandleCreateProduct(productRepo))
}
```

## Executing the Command

When an HTTP or gRPC request comes in, you simply build a typed `command.Context` and execute the command through the bus.

```go
// Inside your HTTP/gRPC endpoint...

func (api *Server) CreateProductEndpoint(w http.ResponseWriter, r *http.Request) {
    // Parse JSON into the command struct
    cmd := commands.CreateProduct{
        ProductID: "12345",
        Name:      "Wireless Mouse",
        Price:     2999,
    }

    // Create a strict Command Context containing metadata about who executed this
    actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:user:999")}
    cmdID := flux.MustParseIdentifier("urn:cmd:uuid-v4-here")
    
    cmdCtx := command.NewContext(r.Context(), cmdID, actor, flux.Identifier{})

    // Execute the command through the bus
    err := command.Execute(cmdCtx, api.CmdBus, cmd)
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    w.WriteHeader(http.StatusCreated)
}
```

### Why use a Bus?

You might wonder why we don't just call `HandleCreateProduct` directly from the HTTP handler.

By passing the command through the `command.Bus`, you gain the ability to use **Middleware (Interceptors)**. You can globally wrap every command execution in your entire application with a single middleware to handle things like:
- Centralized logging ("User 999 executed CreateProduct")
- OpenTelemetry tracing spans
- Database transaction management
- Global error handling

In the next chapter, we'll quickly explore the Sales Domain before diving into how we query this data using Projections!
