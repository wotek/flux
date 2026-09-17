package command

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/domain/order"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
)

// CancelOrder is a command instructing the system to cancel an order.
type CancelOrder struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

// CancelOrderHandler executes the [CancelOrder] command against the order repository.
type CancelOrderHandler struct {
	repo *flux.AggregateRepository[*order.OrderAggregate, order.OrderEvent]
}

// NewCancelOrderHandler creates a new [CancelOrderHandler].
func NewCancelOrderHandler(repo *flux.AggregateRepository[*order.OrderAggregate, order.OrderEvent]) *CancelOrderHandler {
	return &CancelOrderHandler{repo: repo}
}

// Handle executes the [CancelOrder] command.
func (h *CancelOrderHandler) Handle(ctx command.Context, cmd CancelOrder) error {
	stream := flux.Stream{Identifier: identity.NewOrderIdentifier(cmd.OrderID)}
	o, err := h.repo.Load(ctx, stream)
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
