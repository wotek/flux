package order

import (
	"errors"
	"fmt"
	"slices"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/e-commerce/internal/domain/types"
)

var (
	// ErrEmptyCustomerID indicates that an order was placed without a customer identifier.
	ErrEmptyCustomerID = errors.New("customer id cannot be empty")

	// ErrNoLineItems indicates that an order was placed without any line items.
	ErrNoLineItems = errors.New("order must contain at least one line item")

	// ErrInvalidItemQuantity indicates that an item quantity is not positive.
	ErrInvalidItemQuantity = errors.New("item quantity must be positive")

	// ErrOrderNotOpen indicates an operation requires the order to be in open status.
	ErrOrderNotOpen = errors.New("order is not in open status")
)

// OrderAggregate tracks the items, customer association, and lifecycle statuses of a purchase order.
type OrderAggregate struct {
	flux.AggregateRoot[OrderEvent]
	customerID string
	items      []types.LineItem
	status     OrderStatus
}

// New creates an uninitialized [OrderAggregate] bound to the given stream.
func (o *OrderAggregate) New(stream flux.Stream) *OrderAggregate {
	newO := &OrderAggregate{
		items:  make([]types.LineItem, 0),
		status: OrderStatusOpen,
	}
	newO.AggregateRoot = flux.NewAggregateRoot[OrderEvent](stream, flux.NewChangeset[OrderEvent](), newO.apply)
	return newO
}

// CustomerID returns the purchasing customer's identifier.
func (o *OrderAggregate) CustomerID() string {
	return o.customerID
}

// Items returns a copy of the order's line items.
func (o *OrderAggregate) Items() []types.LineItem {
	return slices.Clone(o.items)
}

// Status returns the current lifecycle status of the order.
func (o *OrderAggregate) Status() OrderStatus {
	return o.status
}

// Place validates the order items and customer ID, then records the [OrderPlaced] event.
func (o *OrderAggregate) Place(customerID string, items []types.LineItem) error {
	if customerID == "" {
		return ErrEmptyCustomerID
	}
	if len(items) == 0 {
		return ErrNoLineItems
	}
	for _, item := range items {
		if item.Quantity <= 0 {
			return fmt.Errorf("%w: product %s has quantity %d", ErrInvalidItemQuantity, item.ProductID, item.Quantity)
		}
	}

	o.record(OrderPlaced{
		CustomerID: customerID,
		Items:      slices.Clone(items),
	})
	return nil
}

// Pay marks an open order as paid, recording the [OrderPaid] event.
func (o *OrderAggregate) Pay() error {
	if o.status != OrderStatusOpen {
		return fmt.Errorf("%w: current status %s", ErrOrderNotOpen, o.status)
	}
	o.record(OrderPaid{})
	return nil
}

// Cancel marks an open order as cancelled, recording the [OrderCancelled] event.
func (o *OrderAggregate) Cancel(reason string) error {
	if o.status != OrderStatusOpen {
		return fmt.Errorf("%w: current status %s", ErrOrderNotOpen, o.status)
	}
	o.record(OrderCancelled{
		Reason: reason,
	})
	return nil
}

func (o *OrderAggregate) record(evt OrderEvent) {
	o.Changeset().Record(evt)
	_ = o.apply(evt)
}

func (o *OrderAggregate) apply(evt OrderEvent) error {
	switch e := evt.(type) {
	case OrderPlaced:
		o.customerID = e.CustomerID
		o.items = slices.Clone(e.Items)
		o.status = OrderStatusOpen
	case OrderPaid:
		o.status = OrderStatusPaid
	case OrderCancelled:
		o.status = OrderStatusCancelled
	default:
		return fmt.Errorf("unhandled order event: %s", evt.Name())
	}
	return nil
}
