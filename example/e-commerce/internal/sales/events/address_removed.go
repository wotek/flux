package events

// AddressRemoved is emitted when an address is removed from a customer profile.
type AddressRemoved struct {
	AddressID string `json:"address_id"`
}

// Name returns the event name.
func (AddressRemoved) Name() string {
	return "AddressRemoved"
}

func (AddressRemoved) isCustomerEvent() {}
