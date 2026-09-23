package events

// ProductRenamed is emitted when a product's name is updated.
type ProductRenamed struct {
	ProductName string `json:"product_name"`
}

// Name returns the event name.
func (ProductRenamed) Name() string {
	return "ProductRenamed"
}

func (ProductRenamed) isProductEvent() {}
