package events

// OrderPaid is emitted when payment succeeds.
type OrderPaid struct {
	OrderID string `json:"order_id"`
}

func (e OrderPaid) Name() string { return "OrderPaid" }
func (OrderPaid) isOrderEvent()  {}
