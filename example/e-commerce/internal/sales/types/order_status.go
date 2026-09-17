package types

// OrderStatus defines the lifecycle status of an order.
type OrderStatus string

const (
	// OrderStatusOpen indicates an order has been placed and awaits payment.
	OrderStatusOpen OrderStatus = "open"

	// OrderStatusPaid indicates payment has been confirmed.
	OrderStatusPaid OrderStatus = "paid"

	// OrderStatusCancelled indicates the order was cancelled or timed out.
	OrderStatusCancelled OrderStatus = "cancelled"
)
