# 2. Your First Aggregate

With our project structure in place, we are ready to build the core of our Catalog domain: the **Product** aggregate.

In an event-sourced system, an Aggregate is responsible for validating incoming commands, enforcing business rules (invariants), and emitting **Events** when state changes.

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

## 2. Structuring the Aggregate

Next, we define the `Product` aggregate itself. 

Create `internal/catalog/aggregates/product/product.go`:

```go
package product

import (
	"errors"
	"github.com/wotek/flux"
	"e-commerce/internal/catalog/events"
)

var (
	ErrProductAlreadyCreated = errors.New("product already created")
	ErrInvalidPrice          = errors.New("price must be greater than zero")
)

// Product represents a single item in our e-commerce catalog.
type Product struct {
	// 1. Embed the generic AggregateRoot
	flux.AggregateRoot[flux.Event]

	// 2. Define the read-only derived state
	name    string
	price   int
	created bool
}
```

By embedding `flux.AggregateRoot[flux.Event]`, our struct automatically inherits everything it needs to track uncommitted events and manage versioning.

## 3. The Factory and Mutator

Whenever `flux` loads an aggregate from the database, it needs a way to instantiate an empty version of it (`New()`) and a way to rebuild its state from historical events (`apply()`).

Add the following to `product.go`:

```go
// New is required by the framework to instantiate empty instances during rehydration.
func (p *Product) New(stream flux.Stream) *Product {
	return New(stream)
}

// New is our domain-level constructor.
func New(stream flux.Stream) *Product {
	p := &Product{}
	p.AggregateRoot = flux.NewAggregateRoot[flux.Event](stream, flux.NewChangeset[flux.Event](), p.apply)
	return p
}

// apply is the ONLY place in the entire application where the Product's state is mutated.
func (p *Product) apply(event flux.Event) error {
	switch e := event.(type) {
	case events.ProductCreated:
		p.name = e.Name
		p.price = e.Price
		p.created = true
	case events.PriceUpdated:
		p.price = e.NewPrice
	}
	return nil
}
```

::: warning Critical Rule
The `apply()` method should **never** contain business logic, validation, or conditional statements. By the time an event reaches `apply()`, it is a historical fact that has already happened. It must be unconditionally applied.
:::

## 4. Business Logic and Invariants

Finally, we expose public methods to interact with the aggregate. This is where we validate inputs and enforce business rules (invariants).

If an action is invalid, we return a domain error. If it is valid, we **Record** the event.

```go
// Create initializes a new product.
func (p *Product) Create(name string, price int) error {
	if p.created {
		return ErrProductAlreadyCreated
	}
	if price <= 0 {
		return ErrInvalidPrice
	}

	// Record the event. The framework automatically routes this to apply().
	p.Changeset().Record(events.ProductCreated{
		Name:  name,
		Price: price,
	})
	
	return nil
}

// UpdatePrice changes the product's price.
func (p *Product) UpdatePrice(newPrice int) error {
	if !p.created {
		return errors.New("cannot update price of a non-existent product")
	}
	if newPrice <= 0 {
		return ErrInvalidPrice
	}
	if p.price == newPrice {
		return nil // No state change required
	}

	p.Changeset().Record(events.PriceUpdated{
		OldPrice: p.price,
		NewPrice: newPrice,
	})

	return nil
}
```

Notice how clean this is? The public methods do not mutate `p.name` or `p.price`. They evaluate the rules, and if the rules pass, they emit a fact. The `apply()` method handles the actual state mutation.

In the next chapter, we will learn how to test this aggregate and save it to a database using an `AggregateRepository`.
