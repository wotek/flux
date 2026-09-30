package events

// OrderPlaced is emitted when a customer places an order.
type OrderPlaced struct {
	OrderID string `json:"order_id"`
}

func (e OrderPlaced) Name() string { return "OrderPlaced" }
func (OrderPlaced) isOrderEvent()  {}
