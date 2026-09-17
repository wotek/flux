package order

import (
	"github.com/wotek/flux/example/e-commerce/internal/domain/types"
)

// OrderPlaced is emitted when a customer places a purchase order.
type OrderPlaced struct {
	CustomerID string           `json:"customer_id"`
	Items      []types.LineItem `json:"items"`
}

// Name returns the canonical domain event name.
func (OrderPlaced) Name() string {
	return "shop.order.placed"
}

func (OrderPlaced) isOrderEvent() {}
