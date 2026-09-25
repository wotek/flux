package order

import (
	"errors"
	"fmt"
	"slices"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/e-commerce/internal/sales/events"
	"github.com/wotek/flux/example/e-commerce/internal/sales/types"
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

var _ flux.Aggregate[*OrderAggregate, events.OrderEvent] = (*OrderAggregate)(nil)

// OrderAggregate tracks the items, customer association, and lifecycle statuses of a purchase order.
type OrderAggregate struct {
	flux.AggregateRoot[events.OrderEvent]
	customerID string
	items      []types.LineItem
	status     types.OrderStatus
}

// New creates an uninitialized [OrderAggregate] bound to the given stream.
func (o *OrderAggregate) New(stream flux.Stream) *OrderAggregate {
	newO := &OrderAggregate{
		items:  make([]types.LineItem, 0),
		status: types.OrderStatusOpen,
	}
	newO.AggregateRoot = flux.NewAggregateRoot[events.OrderEvent](stream, flux.NewChangeset[events.OrderEvent](), newO.apply)
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
func (o *OrderAggregate) Status() types.OrderStatus {
	return o.status
}

// Place validates the order items and customer ID, then records the [events.OrderPlaced] event.
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

	evt := events.OrderPlaced{
		CustomerID: customerID,
		Items:      slices.Clone(items),
	}
	o.apply(evt)
	o.Changeset().Record(evt)
	return nil
}

// Pay marks an open order as paid, recording the [events.OrderPaid] event.
func (o *OrderAggregate) Pay() error {
	if o.status != types.OrderStatusOpen {
		return fmt.Errorf("%w: current status %s", ErrOrderNotOpen, o.status)
	}
	evt := events.OrderPaid{}
	o.apply(evt)
	o.Changeset().Record(evt)
	return nil
}

// Cancel marks an open order as cancelled, recording the [events.OrderCancelled] event.
func (o *OrderAggregate) Cancel(reason string) error {
	if o.status != types.OrderStatusOpen {
		return fmt.Errorf("%w: current status %s", ErrOrderNotOpen, o.status)
	}
	evt := events.OrderCancelled{
		Reason: reason,
	}
	o.apply(evt)
	o.Changeset().Record(evt)
	return nil
}

func (o *OrderAggregate) apply(evt events.OrderEvent) {
	switch e := evt.(type) {
	case events.OrderPlaced:
		o.customerID = e.CustomerID
		o.items = slices.Clone(e.Items)
		o.status = types.OrderStatusOpen
	case events.OrderPaid:
		o.status = types.OrderStatusPaid
	case events.OrderCancelled:
		o.status = types.OrderStatusCancelled
	}
}
