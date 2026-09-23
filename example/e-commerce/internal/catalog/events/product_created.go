package events

// ProductCreated is emitted when a new product is added to the catalog.
type ProductCreated struct {
	ProductName string `json:"product_name"`
	Stock       int    `json:"stock"`
}

// Name returns the event name.
func (ProductCreated) Name() string {
	return "ProductCreated"
}

func (ProductCreated) isProductEvent() {}
