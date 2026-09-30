package commands

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/aggregates/order"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/events"
)

// PayOrder marks an order as paid.
type PayOrder struct {
	OrderID string `json:"order_id"`
}

// PayOrderHandler processes [PayOrder].
type PayOrderHandler struct {
	repo *flux.AggregateRepository[*order.OrderAggregate, events.OrderEvent]
}

// NewPayOrderHandler creates a [PayOrderHandler].
func NewPayOrderHandler(repo *flux.AggregateRepository[*order.OrderAggregate, events.OrderEvent]) *PayOrderHandler {
	return &PayOrderHandler{repo: repo}
}

// Handle executes [PayOrder].
func (h *PayOrderHandler) Handle(ctx command.Context, cmd PayOrder) error {
	o, err := h.repo.Load(ctx, order.StreamFor(cmd.OrderID))
	if err != nil {
		return fmt.Errorf("loading order %s: %w", cmd.OrderID, err)
	}
	if err := o.Pay(); err != nil {
		return fmt.Errorf("paying order %s: %w", cmd.OrderID, err)
	}
	if err := h.repo.Save(ctx, o); err != nil {
		return fmt.Errorf("saving order %s: %w", cmd.OrderID, err)
	}
	return nil
}
