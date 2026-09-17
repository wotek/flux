package command

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/domain/customer"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
)

// RemoveAddress is a command instructing the system to remove an address from a customer's address book.
type RemoveAddress struct {
	CustomerID string `json:"customer_id"`
	AddressID  string `json:"address_id"`
}

// RemoveAddressHandler executes the [RemoveAddress] command against the customer repository.
type RemoveAddressHandler struct {
	repo *flux.AggregateRepository[*customer.CustomerAggregate, customer.CustomerEvent]
}

// NewRemoveAddressHandler creates a new [RemoveAddressHandler].
func NewRemoveAddressHandler(repo *flux.AggregateRepository[*customer.CustomerAggregate, customer.CustomerEvent]) *RemoveAddressHandler {
	return &RemoveAddressHandler{repo: repo}
}

// Handle executes the [RemoveAddress] command.
func (h *RemoveAddressHandler) Handle(ctx command.Context, cmd RemoveAddress) error {
	stream := flux.Stream{Identifier: identity.NewCustomerIdentifier(cmd.CustomerID)}
	c, err := h.repo.Load(ctx, stream)
	if err != nil {
		return fmt.Errorf("loading customer %s: %w", cmd.CustomerID, err)
	}

	if err := c.RemoveAddress(cmd.AddressID); err != nil {
		return fmt.Errorf("removing address %s from customer %s: %w", cmd.AddressID, cmd.CustomerID, err)
	}

	if err := h.repo.Save(ctx, c); err != nil {
		return fmt.Errorf("saving customer %s: %w", cmd.CustomerID, err)
	}
	return nil
}
