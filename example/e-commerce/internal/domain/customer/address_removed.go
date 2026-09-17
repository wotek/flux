package customer

// AddressRemoved is emitted when an address is removed from the customer address book.
type AddressRemoved struct {
	AddressID string `json:"address_id"`
}

// Name returns the canonical domain event name.
func (AddressRemoved) Name() string {
	return "shop.customer.address_removed"
}

func (AddressRemoved) isCustomerEvent() {}
