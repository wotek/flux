package events

// OrderCancelled is emitted when an order is cancelled.
type OrderCancelled struct {
	Reason string `json:"reason"`
}

// Name returns the event name.
func (OrderCancelled) Name() string {
	return "OrderCancelled"
}

func (OrderCancelled) isOrderEvent() {}
