package payment

import (
	"slices"

	"github.com/wotek/flux/example/e-commerce/internal/command"
	"github.com/wotek/flux/example/e-commerce/internal/domain/order"
	"github.com/wotek/flux/saga"
)

// RegisterPaymentSaga configures orchestrator routing and handlers for the payment workflow.
func RegisterPaymentSaga(
	o *saga.Orchestrator,
	store saga.Store[*PaymentSaga],
) {
	// 1. OrderPlaced: Captures LineItems to state
	saga.RegisterHandler(o, store, func(ctx saga.Context, s *PaymentSaga, e order.OrderPlaced) error {
		s.ID = ctx.CorrelationIdentifier()
		s.Items = slices.Clone(e.Items)
		return nil
	})

	// 2. OrderPaid: Marks saga as successfully completed
	saga.RegisterHandler(o, store, func(ctx saga.Context, s *PaymentSaga, e order.OrderPaid) error {
		s.IsPaid = true
		return nil
	})

	// 3. OrderCancelled: Intercepts cancellation and triggers stock compensation
	saga.RegisterHandler(o, store, func(ctx saga.Context, s *PaymentSaga, e order.OrderCancelled) error {
		if s.IsPaid || s.IsCancelled {
			return nil
		}
		s.IsCancelled = true

		// Return reserved stock for each item in the order
		for _, item := range s.Items {
			saga.EnqueueCommand(ctx, command.AdjustStock{
				ProductID: item.ProductID,
				Quantity:  item.Quantity, // Positive quantity adds stock back
			})
		}
		return nil
	})

	// 4. PaymentTimeout: If not yet paid or cancelled, issues CancelOrder command and stock compensation
	saga.RegisterHandler(o, store, func(ctx saga.Context, s *PaymentSaga, e PaymentTimeout) error {
		if s.IsPaid || s.IsCancelled {
			return nil
		}
		s.IsCancelled = true

		orderID := e.OrderID
		if orderID == "" && !s.ID.IsEmpty() {
			orderID = s.ID.ResourceID()
		}
		if orderID == "" && !ctx.CorrelationIdentifier().IsEmpty() {
			orderID = ctx.CorrelationIdentifier().ResourceID()
		}

		saga.EnqueueCommand(ctx, command.CancelOrder{
			OrderID: orderID,
			Reason:  "payment window expired",
		})

		for _, item := range s.Items {
			saga.EnqueueCommand(ctx, command.AdjustStock{
				ProductID: item.ProductID,
				Quantity:  item.Quantity, // Positive quantity adds stock back
			})
		}
		return nil
	})
}
