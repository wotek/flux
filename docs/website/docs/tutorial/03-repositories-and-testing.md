# 3. Repositories & Testing

Now that we have our `Product` aggregate, we need a way to save and load it.

## The Aggregate Repository

In your `cmd/server/main.go`, we will initialize an `EventStore` and bind it to a typed `AggregateRepository`:

```go
eventStore := eventstore.New() // Using In-Memory for now
productRepo := flux.NewAggregateRepository[*product.Aggregate, flux.Event](eventStore)
```

## Testing our Aggregate

Because `flux` enforces a strict separation between state evaluation (public methods) and state mutation (`apply()`), unit testing becomes a breeze.

```go
func TestProduct_Create(t *testing.T) {
    p := product.New(flux.Stream{Identifier: flux.MustParseIdentifier("urn:catalog:product:1")})
    
    p.Create("Laptop", 1500)
    
    // Assert the exact event was recorded!
    events := p.Changeset().Events()
    if len(events) != 1 || events[0].Name() != "ProductCreated" {
        t.Fatal("expected ProductCreated event")
    }
}
```
