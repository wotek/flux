package product

// ProductRenamed is emitted when a product name is updated.
type ProductRenamed struct {
	ProductName string `json:"product_name"`
}

// Name returns the canonical domain event name.
func (ProductRenamed) Name() string {
	return "shop.product.renamed"
}

func (ProductRenamed) isProductEvent() {}
