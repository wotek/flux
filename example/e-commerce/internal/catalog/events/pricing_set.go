package events

// PricingSet is emitted when a product's price is updated in minor currency units (cents).
type PricingSet struct {
	Price int `json:"price"`
}

// Name returns the event name.
func (PricingSet) Name() string {
	return "PricingSet"
}

func (PricingSet) isPricingEvent() {}
