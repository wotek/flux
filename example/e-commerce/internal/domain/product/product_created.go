package product

// ProductCreated is emitted when a new product aggregate is initialized.
type ProductCreated struct {
	ProductName string `json:"product_name"`
	Stock       int    `json:"stock"`
}

// Name returns the canonical domain event name.
func (ProductCreated) Name() string {
	return "shop.product.created"
}

func (ProductCreated) isProductEvent() {}
