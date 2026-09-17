package order

// OrderPaid is emitted when payment for an order has been confirmed.
type OrderPaid struct{}

// Name returns the canonical domain event name.
func (OrderPaid) Name() string {
	return "shop.order.paid"
}

func (OrderPaid) isOrderEvent() {}
