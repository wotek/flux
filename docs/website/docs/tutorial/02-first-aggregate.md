# 2. Your First Aggregate

With our project structure in place, we are ready to build the core of our Catalog domain: the **Product** aggregate.

In an event-sourced system, an Aggregate is the ultimate transactional boundary. It is responsible for validating incoming commands, enforcing business rules (invariants), and emitting **Events** when state changes.

We will explicitly follow the architecture rule from the previous chapter: the aggregate embeds `flux.AggregateRoot`, but its purely data-driven state is isolated in a nested `types` subpackage.

## 1. Defining the Events

Before we write the logic, we must define the facts that can occur in a Product's lifecycle. We will start with two events: creating a product and updating its price.

Create a new file at `internal/catalog/events/product.go`:

```go
package events

// ProductCreated is emitted when a new product is added to the catalog.
type ProductCreated struct {
	Name  string
	Price int // We use integers for cents to avoid floating point errors
}

func (e ProductCreated) Name() string { return "ProductCreated" }

// PriceUpdated is emitted when a product's price changes.
type PriceUpdated struct {
	OldPrice int
	NewPrice int
}

func (e PriceUpdated) Name() string { return "PriceUpdated" }
```

Notice that these are simple Go structs that implement the `flux.Event` interface by providing a static `Name()` method.

## 2. Isolating the Aggregate State

Create `internal/catalog/aggregates/product/types/product.go`. 

This is the pure data struct representing the product state. By decoupling this from the framework logic, we ensure that if we ever need to serialize the Aggregate for **Snapshots**, we are only serializing the pure state, not the framework internals.

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
	// Embed the generic AggregateRoot
	flux.AggregateRoot[flux.Event]
	
	// Embed our isolated state struct
	State types.Product
}
```

By embedding `flux.AggregateRoot[flux.Event]`, our struct automatically inherits everything it needs to track uncommitted events and manage versioning.

## 4. The Factory and Mutator

Whenever `flux` loads an aggregate from the database, it needs a way to instantiate an empty version of it (`New()`) and a way to rebuild its state from historical events (`apply()`).

```go
// New is required by the framework to instantiate empty instances during rehydration.
func (a *Aggregate) New(stream flux.Stream) *Aggregate {
	return New(stream)
}

// New is our domain-level constructor.
func (a *Aggregate) New(stream flux.Stream) *Aggregate {
	a := &Aggregate{}
	a.AggregateRoot = flux.NewAggregateRoot[flux.Event](stream, flux.NewChangeset[flux.Event](), a.apply)
	return a
}

// apply is the ONLY place in the entire application where the Product's state is mutated.
func (a *Aggregate) apply(event flux.Event) {
	switch e := event.(type) {
	case events.ProductCreated:
		a.State.Name = e.Name
		a.State.Price = e.Price
		a.State.Created = true
	case events.PriceUpdated:
		a.State.Price = e.NewPrice
	}
}
```

::: warning Critical Rule
The `apply()` method should **never** contain business logic, validation, or conditional statements. By the time an event reaches `apply()`, it is a historical fact that has already happened. It must be unconditionally applied.
:::

## 5. Business Logic and Invariants

Finally, we expose public methods to interact with the aggregate. This is where we validate inputs and enforce business rules (invariants).

If an action is invalid, we return a domain error. If it is valid, we mutate state via `apply` and record the event to the `Changeset`.

```go
// Create enforces invariants and emits the event.
func (a *Aggregate) Create(name string, price int) error {
	if a.State.Created {
		return ErrProductAlreadyCreated
	}
	if price <= 0 {
		return ErrInvalidPrice
	}

	evt := events.ProductCreated{Name: name, Price: price}
	a.apply(evt)
	a.Changeset().Record(evt)
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

	evt := events.PriceUpdated{
		OldPrice: a.State.Price,
		NewPrice: newPrice,
	}
	a.apply(evt)
	a.Changeset().Record(evt)
	return nil
}
```

Notice how clean this is? The public methods do not mutate `a.State.Price` directly. They evaluate the rules, and if the rules pass, they emit a fact. The `apply()` method handles the actual state mutation.

In the next chapter, we will learn how to test this aggregate and save it to a database using an `AggregateRepository`.
