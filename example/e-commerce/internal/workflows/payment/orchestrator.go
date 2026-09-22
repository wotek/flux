package payment

import (
	"slices"

	catalogcommands "github.com/wotek/flux/example/e-commerce/internal/catalog/commands"
	salescommands "github.com/wotek/flux/example/e-commerce/internal/sales/commands"
	salesevents "github.com/wotek/flux/example/e-commerce/internal/sales/events"
	"github.com/wotek/flux/workflow"
)

// RegisterPaymentWorkflow configures orchestrator routing and handlers for the payment workflow.
func RegisterPaymentWorkflow(
	o *workflow.Orchestrator,
	store workflow.Store[*PaymentWorkflow],
) {
	// 1. OrderPlaced: Captures LineItems to state
	workflow.RegisterHandler(o, store, func(ctx workflow.Context, w *PaymentWorkflow, e salesevents.OrderPlaced) error {
		w.ID = ctx.CorrelationIdentifier()
		w.Items = slices.Clone(e.Items)
		return nil
	})

	// 2. OrderPaid: Marks workflow as successfully completed
	workflow.RegisterHandler(o, store, func(ctx workflow.Context, w *PaymentWorkflow, e salesevents.OrderPaid) error {
		w.IsPaid = true
		return nil
	})

	// 3. OrderCancelled: Intercepts cancellation and triggers stock compensation
	workflow.RegisterHandler(o, store, func(ctx workflow.Context, w *PaymentWorkflow, e salesevents.OrderCancelled) error {
		if w.IsPaid || w.IsCancelled {
			return nil
		}
		w.IsCancelled = true

		// Return reserved stock for each item in the order
		for _, item := range w.Items {
			workflow.EnqueueCommand(ctx, catalogcommands.AdjustStock{
				ProductID: item.ProductID,
				Quantity:  item.Quantity, // Positive quantity adds stock back
			})
		}
		return nil
	})

	// 4. PaymentTimeout: If not yet paid or cancelled, issues CancelOrder command and stock compensation
	workflow.RegisterHandler(o, store, func(ctx workflow.Context, w *PaymentWorkflow, e PaymentTimeout) error {
		if w.IsPaid || w.IsCancelled {
			return nil
		}
		w.IsCancelled = true

		orderID := e.OrderID
		if orderID == "" && !w.ID.IsEmpty() {
			orderID = w.ID.ResourceID()
		}
		if orderID == "" && !ctx.CorrelationIdentifier().IsEmpty() {
			orderID = ctx.CorrelationIdentifier().ResourceID()
		}

		workflow.EnqueueCommand(ctx, salescommands.CancelOrder{
			OrderID: orderID,
			Reason:  "payment window expired",
		})

		for _, item := range w.Items {
			workflow.EnqueueCommand(ctx, catalogcommands.AdjustStock{
				ProductID: item.ProductID,
				Quantity:  item.Quantity, // Positive quantity adds stock back
			})
		}
		return nil
	})
}
