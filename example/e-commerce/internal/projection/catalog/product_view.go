package catalog

// ProductView represents the combined read-model state of a product and its pricing.
type ProductView struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Price int    `json:"price"` // in cents
	Stock int    `json:"stock"`
}
