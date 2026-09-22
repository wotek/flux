# 6. Building Projections

Our Write Model (Aggregates) is solid, but we need a fast way to query products for the frontend UI. We do this by building a Projection (Read Model).

## The Projector

A projector listens to Domain Events and updates a database table.

```go
// internal/catalog/projections/product_view.go
func UpdateProductView(ctx event.Context, e catalogevents.ProductCreated) error {
    // INSERT INTO product_view (id, name, price) VALUES (e.ID, e.Name, e.Price)
    return nil
}
```

We bind this to the global EventBus:

```go
eventBus := event.New()
event.Register(eventBus, UpdateProductView)
```
