package order

import (
	"errors"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/events"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/types"
)

var (
	// ErrOrderNotOpen indicates an operation requires the order to be open.
	ErrOrderNotOpen = errors.New("order is not in open status")
)

var _ flux.Aggregate[*OrderAggregate, events.OrderEvent] = (*OrderAggregate)(nil)

// OrderAggregate tracks order lifecycle for the sales write model.
type OrderAggregate struct {
	flux.AggregateRoot[events.OrderEvent]
	orderID string
	status  types.OrderStatus
}

// New creates an uninitialized [OrderAggregate] bound to the given stream.
func (o *OrderAggregate) New(stream flux.Stream) *OrderAggregate {
	newO := &OrderAggregate{}
	newO.AggregateRoot = flux.NewAggregateRoot[events.OrderEvent](stream, flux.NewChangeset[events.OrderEvent](), newO.apply)
	return newO
}

// OrderID returns the business order identifier.
func (o *OrderAggregate) OrderID() string { return o.orderID }

// Status returns the current lifecycle status.
func (o *OrderAggregate) Status() types.OrderStatus { return o.status }

func (o *OrderAggregate) apply(e events.OrderEvent) {
	switch ev := e.(type) {
	case events.OrderPlaced:
		o.orderID = ev.OrderID
		o.status = types.OrderStatusOpen
	case events.OrderPaid:
		o.status = types.OrderStatusPaid
	case events.OrderCancelled:
		o.status = types.OrderStatusCancelled
	}
}

// Place records [events.OrderPlaced]. Idempotent if already placed.
func (o *OrderAggregate) Place(orderID string) error {
	if o.status != types.OrderStatusUnspecified {
		return nil
	}
	evt := events.OrderPlaced{OrderID: orderID}
	o.apply(evt)
	o.Changeset().Record(evt)
	return nil
}

// Pay records [events.OrderPaid]. No-op if already paid.
func (o *OrderAggregate) Pay() error {
	if o.status == types.OrderStatusPaid {
		return nil
	}
	if o.status != types.OrderStatusOpen {
		return ErrOrderNotOpen
	}
	evt := events.OrderPaid{OrderID: o.orderID}
	o.apply(evt)
	o.Changeset().Record(evt)
	return nil
}

// Cancel records [events.OrderCancelled]. No-op if already cancelled or paid.
func (o *OrderAggregate) Cancel(reason string) error {
	if o.status == types.OrderStatusCancelled || o.status == types.OrderStatusPaid {
		return nil
	}
	if o.status != types.OrderStatusOpen {
		return ErrOrderNotOpen
	}
	evt := events.OrderCancelled{OrderID: o.orderID, Reason: reason}
	o.apply(evt)
	o.Changeset().Record(evt)
	return nil
}

// StreamFor returns the event stream for an order ID.
func StreamFor(orderID string) flux.Stream {
	return flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:shop:demo:orders:1:order:" + orderID),
	}
}
