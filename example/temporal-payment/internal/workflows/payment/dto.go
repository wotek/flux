package payment

// TaskQueue is the Temporal task queue for payment fulfillment workers.
const TaskQueue = "flux-temporal-payment"

// FulfillmentInput is a Temporal-serializable DTO (no flux.Event interfaces).
type FulfillmentInput struct {
	OrderID        string `json:"order_id"`
	ActorURN       string `json:"actor_urn"`
	CorrelationURN string `json:"correlation_urn"`
	CausationURN   string `json:"causation_urn"`
	TraceID        string `json:"trace_id"`
	SpanID         string `json:"span_id"`
	TraceFlags     string `json:"trace_flags"`
}

// CancelOrderInput wraps fulfillment metadata with a cancel reason.
type CancelOrderInput struct {
	FulfillmentInput
	Reason string `json:"reason"`
}

// WorkflowID returns the stable Temporal workflow ID for an order.
func WorkflowID(orderID string) string {
	return "order-fulfillment:" + orderID
}
