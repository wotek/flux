package projections

// ProductView represents a merged read model combining product details and current pricing.
type ProductView struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Price int    `json:"price"`
	Stock int    `json:"stock"`
}
