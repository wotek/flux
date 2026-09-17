package command

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/domain/customer"
	"github.com/wotek/flux/example/e-commerce/internal/domain/types"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
)

// AddAddress is a command instructing the system to add an address to a customer's address book.
type AddAddress struct {
	CustomerID string        `json:"customer_id"`
	Address    types.Address `json:"address"`
}

// AddAddressHandler executes the [AddAddress] command against the customer repository.
type AddAddressHandler struct {
	repo *flux.AggregateRepository[*customer.CustomerAggregate, customer.CustomerEvent]
}

// NewAddAddressHandler creates a new [AddAddressHandler].
func NewAddAddressHandler(repo *flux.AggregateRepository[*customer.CustomerAggregate, customer.CustomerEvent]) *AddAddressHandler {
	return &AddAddressHandler{repo: repo}
}

// Handle executes the [AddAddress] command.
func (h *AddAddressHandler) Handle(ctx command.Context, cmd AddAddress) error {
	stream := flux.Stream{Identifier: identity.NewCustomerIdentifier(cmd.CustomerID)}
	c, err := h.repo.Load(ctx, stream)
	if err != nil {
		return fmt.Errorf("loading customer %s: %w", cmd.CustomerID, err)
	}

	if err := c.AddAddress(cmd.Address); err != nil {
		return fmt.Errorf("adding address to customer %s: %w", cmd.CustomerID, err)
	}

	if err := h.repo.Save(ctx, c); err != nil {
		return fmt.Errorf("saving customer %s: %w", cmd.CustomerID, err)
	}
	return nil
}
