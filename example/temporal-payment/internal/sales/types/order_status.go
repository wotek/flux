package types

// OrderStatus is the lifecycle status of an order.
type OrderStatus string

const (
	OrderStatusUnspecified OrderStatus = ""
	OrderStatusOpen        OrderStatus = "open"
	OrderStatusPaid        OrderStatus = "paid"
	OrderStatusCancelled   OrderStatus = "cancelled"
)
