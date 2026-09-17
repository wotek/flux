package commands

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
	"github.com/wotek/flux/example/e-commerce/internal/sales/aggregates/customer"
	"github.com/wotek/flux/example/e-commerce/internal/sales/events"
	"github.com/wotek/flux/example/e-commerce/internal/types"
)

// AddAddress represents an intent to attach a new address to a customer profile.
type AddAddress struct {
	CustomerID string        `json:"customer_id"`
	Address    types.Address `json:"address"`
}

// AddAddressHandler processes [AddAddress] commands.
type AddAddressHandler struct {
	repo *flux.AggregateRepository[*customer.CustomerAggregate, events.CustomerEvent]
}

// NewAddAddressHandler creates a handler for [AddAddress].
func NewAddAddressHandler(repo *flux.AggregateRepository[*customer.CustomerAggregate, events.CustomerEvent]) *AddAddressHandler {
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
		return fmt.Errorf("adding address for customer %s: %w", cmd.CustomerID, err)
	}

	if err := h.repo.Save(ctx, c); err != nil {
		return fmt.Errorf("saving customer %s: %w", cmd.CustomerID, err)
	}
	return nil
}
