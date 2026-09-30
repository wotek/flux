package commands

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/aggregates/order"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/events"
)

// PlaceOrder requests creation of a new order.
type PlaceOrder struct {
	OrderID string `json:"order_id"`
}

// PlaceOrderHandler processes [PlaceOrder].
type PlaceOrderHandler struct {
	repo *flux.AggregateRepository[*order.OrderAggregate, events.OrderEvent]
}

// NewPlaceOrderHandler creates a [PlaceOrderHandler].
func NewPlaceOrderHandler(repo *flux.AggregateRepository[*order.OrderAggregate, events.OrderEvent]) *PlaceOrderHandler {
	return &PlaceOrderHandler{repo: repo}
}

// Handle executes [PlaceOrder].
func (h *PlaceOrderHandler) Handle(ctx command.Context, cmd PlaceOrder) error {
	o := (*order.OrderAggregate)(nil).New(order.StreamFor(cmd.OrderID))
	if err := o.Place(cmd.OrderID); err != nil {
		return fmt.Errorf("placing order %s: %w", cmd.OrderID, err)
	}
	if err := h.repo.Save(ctx, o); err != nil {
		return fmt.Errorf("saving order %s: %w", cmd.OrderID, err)
	}
	return nil
}
