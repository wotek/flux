package commands

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
	"github.com/wotek/flux/example/e-commerce/internal/sales/aggregates/order"
	"github.com/wotek/flux/example/e-commerce/internal/sales/events"
)

// PayOrder represents an intent to confirm payment for an order.
type PayOrder struct {
	OrderID string `json:"order_id"`
}

// PayOrderHandler processes [PayOrder] commands.
type PayOrderHandler struct {
	repo *flux.AggregateRepository[*order.OrderAggregate, events.OrderEvent]
}

// NewPayOrderHandler creates a handler for [PayOrder].
func NewPayOrderHandler(repo *flux.AggregateRepository[*order.OrderAggregate, events.OrderEvent]) *PayOrderHandler {
	return &PayOrderHandler{repo: repo}
}

// Handle executes the [PayOrder] command.
func (h *PayOrderHandler) Handle(ctx command.Context, cmd PayOrder) error {
	stream := flux.Stream{Identifier: identity.NewOrderIdentifier(cmd.OrderID)}
	o, err := h.repo.Load(ctx, stream)
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
