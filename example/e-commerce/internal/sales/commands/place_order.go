package commands

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
	"github.com/wotek/flux/example/e-commerce/internal/sales/aggregates/order"
	"github.com/wotek/flux/example/e-commerce/internal/sales/events"
	"github.com/wotek/flux/example/e-commerce/internal/sales/types"
)

// PlaceOrder represents an intent to place a new order.
type PlaceOrder struct {
	OrderID    string           `json:"order_id"`
	CustomerID string           `json:"customer_id"`
	Items      []types.LineItem `json:"items"`
}

// PlaceOrderHandler processes [PlaceOrder] commands.
type PlaceOrderHandler struct {
	repo *flux.AggregateRepository[*order.OrderAggregate, events.OrderEvent]
}

// NewPlaceOrderHandler creates a handler for [PlaceOrder].
func NewPlaceOrderHandler(repo *flux.AggregateRepository[*order.OrderAggregate, events.OrderEvent]) *PlaceOrderHandler {
	return &PlaceOrderHandler{repo: repo}
}

// Handle executes the [PlaceOrder] command.
func (h *PlaceOrderHandler) Handle(ctx command.Context, cmd PlaceOrder) error {
	stream := flux.Stream{Identifier: identity.NewOrderIdentifier(cmd.OrderID)}
	var zero *order.OrderAggregate
	o := zero.New(stream)

	if err := o.Place(cmd.CustomerID, cmd.Items); err != nil {
		return fmt.Errorf("placing order %s: %w", cmd.OrderID, err)
	}

	if err := h.repo.Save(ctx, o); err != nil {
		return fmt.Errorf("saving order %s: %w", cmd.OrderID, err)
	}
	return nil
}
