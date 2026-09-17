package product

// StockAdjusted is emitted when a product stock level is changed.
type StockAdjusted struct {
	Quantity int `json:"quantity"`
}

// Name returns the canonical domain event name.
func (StockAdjusted) Name() string {
	return "shop.product.stock_adjusted"
}

func (StockAdjusted) isProductEvent() {}
