package command

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/domain/order"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
)

// PayOrder is a command instructing the system to mark an order as paid.
type PayOrder struct {
	OrderID string `json:"order_id"`
}

// PayOrderHandler executes the [PayOrder] command against the order repository.
type PayOrderHandler struct {
	repo *flux.AggregateRepository[*order.OrderAggregate, order.OrderEvent]
}

// NewPayOrderHandler creates a new [PayOrderHandler].
func NewPayOrderHandler(repo *flux.AggregateRepository[*order.OrderAggregate, order.OrderEvent]) *PayOrderHandler {
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
