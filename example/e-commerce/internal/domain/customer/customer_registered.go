package customer

// CustomerRegistered is emitted when a customer profile is registered.
type CustomerRegistered struct {
	CustomerName string `json:"customer_name"`
	Email        string `json:"email"`
}

// Name returns the canonical domain event name.
func (CustomerRegistered) Name() string {
	return "shop.customer.registered"
}

func (CustomerRegistered) isCustomerEvent() {}
