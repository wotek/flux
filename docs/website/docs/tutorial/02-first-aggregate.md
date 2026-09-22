# 2. Your First Aggregate

With our project structure in place, we are ready to build the core of our Catalog domain: the **Product** aggregate.

We will strictly follow the layout rule: the aggregate embeds `flux.AggregateRoot`, but its purely data-driven state is isolated in a nested `types` subpackage.

## 1. Defining the Events

Create a new file at `internal/catalog/events/product.go`:

```go
package events

type ProductCreated struct {
	Name  string
	Price int // Cents
}
func (e ProductCreated) Name() string { return "ProductCreated" }

type PriceUpdated struct {
	OldPrice int
	NewPrice int
}
func (e PriceUpdated) Name() string { return "PriceUpdated" }
```

## 2. Isolating the Aggregate State

Create `internal/catalog/aggregates/product/types/product.go`. This is the pure data struct representing the product state. By isolating this, we can easily serialize it for Snapshots later.

```go
package types

// Product represents the internal write-model state of a Catalog Product.
type Product struct {
	Name    string
	Price   int
	Created bool
}
```

## 3. Structuring the Aggregate

Create `internal/catalog/aggregates/product/aggregate.go`. This struct will manage the `types.Product` state and enforce rules.

```go
package product

import (
	"errors"
	"github.com/wotek/flux"
	"e-commerce/internal/catalog/aggregates/product/types"
	"e-commerce/internal/catalog/events"
)

var (
	ErrProductAlreadyCreated = errors.New("product already created")
	ErrInvalidPrice          = errors.New("price must be greater than zero")
)

type Aggregate struct {
	flux.AggregateRoot[flux.Event]
	State types.Product // The isolated data struct
}

// New initializes the aggregate.
func (a *Aggregate) New(stream flux.Stream) *Aggregate {
	return New(stream)
}

func New(stream flux.Stream) *Aggregate {
	a := &Aggregate{}
	a.AggregateRoot = flux.NewAggregateRoot[flux.Event](stream, flux.NewChangeset[flux.Event](), a.apply)
	return a
}
```

## 4. The Mutator and Business Logic

The `apply()` method is the only place we mutate `a.State`.

```go
func (a *Aggregate) apply(event flux.Event) error {
	switch e := event.(type) {
	case events.ProductCreated:
		a.State.Name = e.Name
		a.State.Price = e.Price
		a.State.Created = true
	case events.PriceUpdated:
		a.State.Price = e.NewPrice
	}
	return nil
}

// Create enforces invariants and emits the event.
func (a *Aggregate) Create(name string, price int) error {
	if a.State.Created {
		return ErrProductAlreadyCreated
	}
	if price <= 0 {
		return ErrInvalidPrice
	}

	a.Changeset().Record(events.ProductCreated{Name: name, Price: price})
	return nil
}

// UpdatePrice changes the product's price.
func (a *Aggregate) UpdatePrice(newPrice int) error {
	if !a.State.Created {
		return errors.New("product does not exist")
	}
	if newPrice <= 0 {
		return ErrInvalidPrice
	}
	if a.State.Price == newPrice {
		return nil // Idempotent skip
	}

	a.Changeset().Record(events.PriceUpdated{
		OldPrice: a.State.Price,
		NewPrice: newPrice,
	})
	return nil
}
```

By decoupling `types.Product` from `product.Aggregate`, we guarantee our CQRS boundary: the Write Model's state is completely hidden from the outside world.

In the next chapter, we will learn how to test this aggregate and save it to a database using an `AggregateRepository`.
