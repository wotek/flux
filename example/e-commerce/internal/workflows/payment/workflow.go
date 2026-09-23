package payment

import (
	"slices"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/e-commerce/internal/sales/types"
	"github.com/wotek/flux/workflow"
)

var _ workflow.Workflow[*PaymentWorkflow] = (*PaymentWorkflow)(nil)

// PaymentWorkflow manages the payment deadline and compensation state machine for an order.
type PaymentWorkflow struct {
	ID          flux.Identifier  `json:"id"`
	Items       []types.LineItem `json:"items"`
	IsPaid      bool             `json:"is_paid"`
	IsCancelled bool             `json:"is_cancelled"`
}

// Identifier returns the unique workflow instance identifier.
func (w *PaymentWorkflow) Identifier() flux.Identifier {
	return w.ID
}

// New creates an uninitialized [PaymentWorkflow] instance.
func (w *PaymentWorkflow) New() *PaymentWorkflow {
	return &PaymentWorkflow{
		Items: make([]types.LineItem, 0),
	}
}

// Clone returns a shallow copy of the workflow state with cloned line items.
func (w *PaymentWorkflow) Clone() *PaymentWorkflow {
	return &PaymentWorkflow{
		ID:          w.ID,
		Items:       slices.Clone(w.Items),
		IsPaid:      w.IsPaid,
		IsCancelled: w.IsCancelled,
	}
}
