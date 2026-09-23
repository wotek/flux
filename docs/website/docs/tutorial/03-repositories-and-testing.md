# 3. Repositories & Testing

In the previous chapter, we built our `Product` aggregate. But an aggregate living purely in memory isn't very useful—we need a way to persistently save its events and fetch it back later.

We do this using the `AggregateRepository`.

## The Aggregate Repository

The `flux.AggregateRepository` acts as the explicit bridge between your in-memory domain aggregates and your persistent `EventStore`. 

Unlike an ORM which saves the *current state* of a struct to a row, the Aggregate Repository has two highly specialized jobs:
1. **Saving:** It extracts the uncommitted events from your aggregate's `Changeset` and appends them to the event stream.
2. **Loading:** It fetches all historical events for a given stream, instantiates an empty aggregate (using `New()`), and replays every event through your `apply()` method to rebuild the state.

### Initializing the Type Registry & Repository

Before you initialize your database driver, you need to register your events. Because `flux` uses generic interfaces, backend drivers need a **Type Registry** to dynamically rebuild your concrete Go structs when reading raw bytes from the database.

In your application's entrypoint (e.g., `cmd/server/main.go`), you will setup the registry, initialize an `EventStore`, and bind it to a generic repository typed specifically for your `Product`.

```go
import (
	"github.com/wotek/flux"
	"github.com/wotek/flux/event"
	"github.com/wotek/flux/codec/json"
	
	"e-commerce/internal/catalog/aggregates/product"
	catalogEvents "e-commerce/internal/catalog/events"
	
	// e.g., if using a persistent driver:
	// redisStore "github.com/wotek/flux/event/store/redis"
	memoryStore "github.com/wotek/flux/event/store"
)

// 1. Initialize the Event Type Registry
registry := event.NewTypes()
event.RegisterType[catalogEvents.ProductCreated](registry)
event.RegisterType[catalogEvents.PriceUpdated](registry)

// 2. Setup your Codec (If using a persistent database)
jsonCodec := json.New(registry)

// 3. Initialize the storage backend
// For in-memory, we don't strictly need the codec, but a real DB does!
store := memoryStore.New()

// 4. Initialize a repository exclusively for Products
repo := flux.NewAggregateRepository[*product.Product, flux.Event](store)
```

## Persisting an Aggregate

When you create a brand new product, you instantiate it, call your domain methods, and then ask the repository to save it.

```go
// Create a new stream identifier
stream := flux.Stream{Identifier: flux.MustParseIdentifier("urn:catalog:product:123")}

// Instantiate a new, empty aggregate
prod := product.New(stream)

// Execute business logic (this Records the ProductCreated event internally)
err := prod.Create("Ergonomic Keyboard", 15000)
if err != nil {
	log.Fatal(err)
}

// Persist it!
// The repository extracts the uncommitted ProductCreated event and saves it.
err = repo.Save(ctx, prod)
```

### Optimistic Concurrency

What happens if two users try to update the exact same product at the exact same millisecond?

Because Event Sourcing requires strict ordering, the repository enforces **Optimistic Concurrency**. When `repo.Save()` is called, it attempts to append the new events at the *exact version* the aggregate was at when it was loaded in memory. If another process modified the stream in the meantime, the database will reject the save and return `flux.ErrConcurrency`.


::: info The Serialization Boundary
Notice that we didn't define any `json` tags on our `ProductCreated` event struct? 
The `flux` framework treats events and envelopes as pure in-memory concepts. When the Repository passes the uncommitted events to the underlying `EventStore` backend (like Redis or MySQL), the backend driver itself is responsible for mapping it into a Data Transfer Object (DTO) and flattening it into JSON bytes. This keeps our domain 100% free of infrastructure concerns!
:::

## Fetching an Aggregate

Fetching an aggregate is just as simple. You provide the stream identifier, and the repository handles the rehydration process.

```go
stream := flux.Stream{Identifier: flux.MustParseIdentifier("urn:catalog:product:123")}

prod, err := repo.Load(ctx, stream)
if errors.Is(err, flux.ErrAggregateNotFound) {
	log.Println("Product does not exist!")
}

// Now you can execute further business logic on the fully rehydrated aggregate
prod.UpdatePrice(12000)
repo.Save(ctx, prod)
```

## Snapshots (Handling Huge Streams)

Imagine a `BankAccount` aggregate that has been open for 10 years and has 500,000 transaction events. If we call `repo.Load()`, the repository has to fetch all 500,000 events and loop them through `apply()`. This is highly inefficient.

To solve this, `flux` supports **Snapshots**.

You can configure your repository with a `SnapshotStore`. Periodically (e.g., every 100 events), you can serialize the current state of your aggregate and save it as a Snapshot.

When `repo.Load()` is called on a snapshot-enabled repository, it will:
1. Load the most recent Snapshot (skipping the first 499,900 events).
2. Fetch *only* the new events that occurred after the snapshot.
3. Replay those final 100 events to bring the state up to the current millisecond.

*(Note: We won't implement snapshots in this basic tutorial, but the framework fully supports them natively!)*

## Testing our Aggregate

One of the greatest benefits of the `flux` architecture is how effortlessly testable it is. Because business logic is entirely decoupled from databases and network I/O, you can unit test your entire domain at lightning speed without mocking a single repository.

You simply instantiate the Aggregate, call the public method, and assert against the resulting `Changeset`.

```go
package product_test

import (
	"testing"
	"github.com/wotek/flux"
	"e-commerce/internal/catalog/aggregates/product"
	"e-commerce/internal/catalog/events"
)

func TestProduct_Create(t *testing.T) {
	// 1. Arrange: Create an empty aggregate
	stream := flux.Stream{Identifier: flux.MustParseIdentifier("urn:catalog:product:1")}
	p := product.New(stream)

	// 2. Act: Execute the business logic
	err := p.Create("Test Product", 5000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 3. Assert: Check the recorded events!
	recordedEvents := p.Changeset().Events()
	if len(recordedEvents) != 1 {
		t.Fatalf("expected 1 event, got %d", len(recordedEvents))
	}

	createdEvent, ok := recordedEvents[0].(events.ProductCreated)
	if !ok {
		t.Fatal("expected event to be ProductCreated")
	}

	if createdEvent.Name != "Test Product" || createdEvent.Price != 5000 {
		t.Error("event data did not match expected values")
	}
}
```

In the next chapter, we'll look at how to hook this all up to a REST/gRPC API using the **Command Bus**.
