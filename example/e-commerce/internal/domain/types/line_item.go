package types

// LineItem represents a purchased product item and its quantity within an order.
type LineItem struct {
	ProductID string `json:"product_id"`
	Name      string `json:"name"`
	Price     int    `json:"price"` // in cents
	Quantity  int    `json:"quantity"`
}
