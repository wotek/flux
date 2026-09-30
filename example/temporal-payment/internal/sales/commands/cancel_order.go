package commands

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/aggregates/order"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/events"
)

// CancelOrder cancels an unpaid order.
type CancelOrder struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

// CancelOrderHandler processes [CancelOrder].
type CancelOrderHandler struct {
	repo *flux.AggregateRepository[*order.OrderAggregate, events.OrderEvent]
}

// NewCancelOrderHandler creates a [CancelOrderHandler].
func NewCancelOrderHandler(repo *flux.AggregateRepository[*order.OrderAggregate, events.OrderEvent]) *CancelOrderHandler {
	return &CancelOrderHandler{repo: repo}
}

// Handle executes [CancelOrder].
func (h *CancelOrderHandler) Handle(ctx command.Context, cmd CancelOrder) error {
	o, err := h.repo.Load(ctx, order.StreamFor(cmd.OrderID))
	if err != nil {
		return fmt.Errorf("loading order %s: %w", cmd.OrderID, err)
	}
	if err := o.Cancel(cmd.Reason); err != nil {
		return fmt.Errorf("cancelling order %s: %w", cmd.OrderID, err)
	}
	if err := h.repo.Save(ctx, o); err != nil {
		return fmt.Errorf("saving order %s: %w", cmd.OrderID, err)
	}
	return nil
}
