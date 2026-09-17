package events

// StockAdjusted is emitted when a product's stock is increased or decreased.
type StockAdjusted struct {
	Quantity int `json:"quantity"`
}

// Name returns the event name.
func (StockAdjusted) Name() string {
	return "StockAdjusted"
}

func (StockAdjusted) isProductEvent() {}
