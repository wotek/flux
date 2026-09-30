package events

// OrderCancelled is emitted when an order is cancelled.
type OrderCancelled struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

func (e OrderCancelled) Name() string { return "OrderCancelled" }
func (OrderCancelled) isOrderEvent()  {}
