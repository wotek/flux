package events

// OrderPaid is emitted when payment confirmation is received for an order.
type OrderPaid struct{}

// Name returns the event name.
func (OrderPaid) Name() string {
	return "OrderPaid"
}

func (OrderPaid) isOrderEvent() {}
