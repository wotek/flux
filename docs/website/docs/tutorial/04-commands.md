# 4. Commands & Command Bus

With our Aggregate and Repository ready, it's time to route requests to it using the CQRS Command Bus.

## Defining the Command

```go
// internal/catalog/commands/create_product.go
type CreateProduct struct {
    ProductID string
    Name      string
    Price     int
}
```

## Registering the Handler

In our `main.go`, we wire the command to a handler that loads the aggregate, executes the business logic, and saves it.

```go
cmdBus := command.New()

command.Register(cmdBus, func(ctx command.Context, cmd CreateProduct) error {
    streamID := flux.MustParseIdentifier(fmt.Sprintf("urn:catalog:product:%s", cmd.ProductID))
    stream := flux.Stream{Identifier: streamID}
    
    agg := product.New(stream)
    agg.Create(cmd.Name, cmd.Price)
    
    return productRepo.Save(ctx, agg)
})
```
