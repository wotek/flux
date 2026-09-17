package payment

// PaymentTimeout is emitted by a timer to signify that payment for an order was not received in time.
type PaymentTimeout struct {
	OrderID string `json:"order_id"`
}

// Name returns the event name.
func (PaymentTimeout) Name() string {
	return "PaymentTimeout"
}
