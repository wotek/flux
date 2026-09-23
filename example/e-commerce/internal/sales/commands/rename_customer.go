package commands

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
	"github.com/wotek/flux/example/e-commerce/internal/sales/aggregates/customer"
	"github.com/wotek/flux/example/e-commerce/internal/sales/events"
)

// RenameCustomer represents an intent to change a customer's name.
type RenameCustomer struct {
	CustomerID string `json:"customer_id"`
	NewName    string `json:"new_name"`
}

// RenameCustomerHandler processes [RenameCustomer] commands.
type RenameCustomerHandler struct {
	repo *flux.AggregateRepository[*customer.CustomerAggregate, events.CustomerEvent]
}

// NewRenameCustomerHandler creates a handler for [RenameCustomer].
func NewRenameCustomerHandler(repo *flux.AggregateRepository[*customer.CustomerAggregate, events.CustomerEvent]) *RenameCustomerHandler {
	return &RenameCustomerHandler{repo: repo}
}

// Handle executes the [RenameCustomer] command.
func (h *RenameCustomerHandler) Handle(ctx command.Context, cmd RenameCustomer) error {
	stream := flux.Stream{Identifier: identity.NewCustomerIdentifier(cmd.CustomerID)}
	c, err := h.repo.Load(ctx, stream)
	if err != nil {
		return fmt.Errorf("loading customer %s: %w", cmd.CustomerID, err)
	}

	if err := c.Rename(cmd.NewName); err != nil {
		return fmt.Errorf("renaming customer %s: %w", cmd.CustomerID, err)
	}

	if err := h.repo.Save(ctx, c); err != nil {
		return fmt.Errorf("saving customer %s: %w", cmd.CustomerID, err)
	}
	return nil
}
