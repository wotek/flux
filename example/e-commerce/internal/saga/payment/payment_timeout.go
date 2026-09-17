package payment

// PaymentTimeout is emitted when an order payment window expires before payment confirmation.
type PaymentTimeout struct {
	OrderID string `json:"order_id"`
}

// Name returns the canonical domain event name.
func (PaymentTimeout) Name() string {
	return "shop.payment.timeout"
}
