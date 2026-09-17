package order

// OrderStatus represents the lifecycle state of a purchase order.
type OrderStatus string

const (
	// OrderStatusOpen indicates that an order has been placed and awaits payment.
	OrderStatusOpen OrderStatus = "open"

	// OrderStatusPaid indicates that payment has been successfully confirmed.
	OrderStatusPaid OrderStatus = "paid"

	// OrderStatusCancelled indicates that the order was cancelled or timed out.
	OrderStatusCancelled OrderStatus = "cancelled"
)
