package command

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/domain/customer"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
)

// RenameCustomer is a command instructing the system to update a customer's name.
type RenameCustomer struct {
	CustomerID string `json:"customer_id"`
	Name       string `json:"name"`
}

// RenameCustomerHandler executes the [RenameCustomer] command against the customer repository.
type RenameCustomerHandler struct {
	repo *flux.AggregateRepository[*customer.CustomerAggregate, customer.CustomerEvent]
}

// NewRenameCustomerHandler creates a new [RenameCustomerHandler].
func NewRenameCustomerHandler(repo *flux.AggregateRepository[*customer.CustomerAggregate, customer.CustomerEvent]) *RenameCustomerHandler {
	return &RenameCustomerHandler{repo: repo}
}

// Handle executes the [RenameCustomer] command.
func (h *RenameCustomerHandler) Handle(ctx command.Context, cmd RenameCustomer) error {
	stream := flux.Stream{Identifier: identity.NewCustomerIdentifier(cmd.CustomerID)}
	c, err := h.repo.Load(ctx, stream)
	if err != nil {
		return fmt.Errorf("loading customer %s: %w", cmd.CustomerID, err)
	}

	if err := c.Rename(cmd.Name); err != nil {
		return fmt.Errorf("renaming customer %s: %w", cmd.CustomerID, err)
	}

	if err := h.repo.Save(ctx, c); err != nil {
		return fmt.Errorf("saving customer %s: %w", cmd.CustomerID, err)
	}
	return nil
}
