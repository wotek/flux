package payment

import (
	"github.com/wotek/flux/event"
)

// TaskQueue is the Temporal task queue for payment fulfillment workers.
const TaskQueue = "flux-temporal-payment"

// CancelOrderInput pairs an event reference with an external cancellation reason.
// The reason is not part of the initiating event, justifying this slim custom DTO.
type CancelOrderInput struct {
	Reference event.EventReference `json:"reference"`
	Reason    string               `json:"reason"`
}

// WorkflowID returns the stable Temporal workflow ID for an order.
func WorkflowID(orderID string) string {
	return "order-fulfillment:" + orderID
}
