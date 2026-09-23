package types

// Address represents a customer postal delivery address.
type Address struct {
	ID      string `json:"id"`
	Street  string `json:"street"`
	City    string `json:"city"`
	ZipCode string `json:"zip_code"`
	Country string `json:"country"`
}
