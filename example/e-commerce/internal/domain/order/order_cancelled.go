package order

// OrderCancelled is emitted when an order is cancelled by the customer or timed out.
type OrderCancelled struct {
	Reason string `json:"reason"`
}

// Name returns the canonical domain event name.
func (OrderCancelled) Name() string {
	return "shop.order.cancelled"
}

func (OrderCancelled) isOrderEvent() {}
