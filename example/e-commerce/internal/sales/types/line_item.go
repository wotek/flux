package types

// LineItem represents an item within a customer order.
type LineItem struct {
	ProductID string `json:"product_id"`
	Name      string `json:"name"`
	Price     int    `json:"price"`
	Quantity  int    `json:"quantity"`
}
