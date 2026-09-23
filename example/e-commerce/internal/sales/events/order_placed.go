package events

import "github.com/wotek/flux/example/e-commerce/internal/sales/types"

// OrderPlaced is emitted when a customer places an order.
type OrderPlaced struct {
	CustomerID string           `json:"customer_id"`
	Items      []types.LineItem `json:"items"`
}

// Name returns the event name.
func (OrderPlaced) Name() string {
	return "OrderPlaced"
}

func (OrderPlaced) isOrderEvent() {}
