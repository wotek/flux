package customer

import (
	"github.com/wotek/flux/example/e-commerce/internal/domain/types"
)

// AddressAdded is emitted when a new address is added to the customer address book.
type AddressAdded struct {
	Address types.Address `json:"address"`
}

// Name returns the canonical domain event name.
func (AddressAdded) Name() string {
	return "shop.customer.address_added"
}

func (AddressAdded) isCustomerEvent() {}
