package pricing

// PricingSet is emitted when a price is set or updated for a product.
type PricingSet struct {
	Price int `json:"price"` // in cents
}

// Name returns the canonical domain event name.
func (PricingSet) Name() string {
	return "shop.pricing.set"
}

func (PricingSet) isPricingEvent() {}
