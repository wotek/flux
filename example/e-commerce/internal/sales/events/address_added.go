package events

import "github.com/wotek/flux/example/e-commerce/internal/types"

// AddressAdded is emitted when an address is added to a customer profile.
type AddressAdded struct {
	Address types.Address `json:"address"`
}

// Name returns the event name.
func (AddressAdded) Name() string {
	return "AddressAdded"
}

func (AddressAdded) isCustomerEvent() {}
