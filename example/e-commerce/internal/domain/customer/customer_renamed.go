package customer

// CustomerRenamed is emitted when a customer name is updated.
type CustomerRenamed struct {
	CustomerName string `json:"customer_name"`
}

// Name returns the canonical domain event name.
func (CustomerRenamed) Name() string {
	return "shop.customer.renamed"
}

func (CustomerRenamed) isCustomerEvent() {}
