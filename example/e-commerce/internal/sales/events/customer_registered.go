package events

// CustomerRegistered is emitted when a new customer signs up.
type CustomerRegistered struct {
	CustomerName string `json:"customer_name"`
	Email        string `json:"email"`
}

// Name returns the event name.
func (CustomerRegistered) Name() string {
	return "CustomerRegistered"
}

func (CustomerRegistered) isCustomerEvent() {}
