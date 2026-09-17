package events

// CustomerRenamed is emitted when a customer changes their registered name.
type CustomerRenamed struct {
	CustomerName string `json:"customer_name"`
}

// Name returns the event name.
func (CustomerRenamed) Name() string {
	return "CustomerRenamed"
}

func (CustomerRenamed) isCustomerEvent() {}
