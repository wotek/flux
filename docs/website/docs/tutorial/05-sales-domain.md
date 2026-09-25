# 5. The Sales Domain

With our `Catalog` domain fully managing products and pricing, we need a new Bounded Context to handle actual purchases. We will call this the **Sales** domain.

In a well-designed microservice or modular monolith architecture, domains should be isolated. The Sales domain shouldn't care about how a Product's price is calculated—it only cares about capturing a Customer's intent to buy.

## 1. The Order Aggregate

The core of the Sales domain is the `Order` aggregate. Unlike the `Product` aggregate which has a very long lifecycle, an `Order` has a distinct, short lifecycle: it is created, items are added, and then it is placed (checkout).

Create `internal/sales/aggregates/order/order.go`:

```go
package order

import (
	"errors"
	"github.com/wotek/flux"
	"e-commerce/internal/sales/events"
)

var (
	ErrOrderAlreadyPlaced = errors.New("cannot modify an order that is already placed")
	ErrNegativeQuantity   = errors.New("item quantity must be greater than zero")
)

type Order struct {
	flux.AggregateRoot[flux.Event]

	customerID string
	items      map[string]int // Map of ProductID to Quantity
	placed     bool
}

// New initializes the aggregate for rehydration.
func (o *Order) New(stream flux.Stream) *Order {
	return New(stream)
}

func New(stream flux.Stream) *Order {
	o := &Order{
		items: make(map[string]int),
	}
	o.AggregateRoot = flux.NewAggregateRoot[flux.Event](stream, flux.NewChangeset[flux.Event](), o.apply)
	return o
}
```

## 2. Managing Order State

Now, let's implement the `apply()` mutator. Notice how we use the map to keep track of item quantities, allowing customers to add the same product multiple times.

```go
func (o *Order) apply(event flux.Event) {
	switch e := event.(type) {
	case events.OrderStarted:
		o.customerID = e.CustomerID
	case events.ItemAdded:
		o.items[e.ProductID] += e.Quantity
	case events.ItemRemoved:
		o.items[e.ProductID] -= e.Quantity
		if o.items[e.ProductID] <= 0 {
			delete(o.items, e.ProductID)
		}
	case events.OrderPlaced:
		o.placed = true
	}
}
```

## 3. Business Invariants

Finally, we implement the public methods. The most critical invariant here is that an Order cannot be modified once it is placed. 

Every action must check `if o.placed` before allowing state changes.

```go
func (o *Order) Start(customerID string) error {
	if o.customerID != "" {
		return errors.New("order is already started")
	}
	
	evt := events.OrderStarted{CustomerID: customerID}
	o.apply(evt)
	o.Changeset().Record(evt)
	return nil
}

func (o *Order) AddItem(productID string, quantity int) error {
	if o.placed {
		return ErrOrderAlreadyPlaced
	}
	if quantity <= 0 {
		return ErrNegativeQuantity
	}

	evt := events.ItemAdded{
		ProductID: productID,
		Quantity:  quantity,
	}
	o.apply(evt)
	o.Changeset().Record(evt)
	return nil
}

func (o *Order) Place() error {
	if o.placed {
		return ErrOrderAlreadyPlaced
	}
	if len(o.items) == 0 {
		return errors.New("cannot place an empty order")
	}

	evt := events.OrderPlaced{}
	o.apply(evt)
	o.Changeset().Record(evt)
	return nil
}
```

## Why Separate Domains?

You might wonder why we didn't just add an `Orders` slice directly to the `Customer` aggregate.

In Event Sourcing, **Aggregates are transactional boundaries**. If a Customer placed 500 orders over their lifetime, and every order was stored inside the `Customer` aggregate, adding a new order would require loading all 500 historical orders to rehydrate the state. 

By separating `Order` into its own aggregate stream (e.g., `urn:sales:order:9876`), we ensure that modifying an order is fast and has zero risk of optimistic concurrency conflicts with other actions the Customer might be taking simultaneously.

In the next chapter, we'll look at how to query data across these domains using Projections.
