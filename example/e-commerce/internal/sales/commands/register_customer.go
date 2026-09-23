package commands

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
	"github.com/wotek/flux/example/e-commerce/internal/sales/aggregates/customer"
	"github.com/wotek/flux/example/e-commerce/internal/sales/events"
)

// RegisterCustomer represents an intent to register a new customer account.
type RegisterCustomer struct {
	CustomerID string `json:"customer_id"`
	Name       string `json:"name"`
	Email      string `json:"email"`
}

// RegisterCustomerHandler processes [RegisterCustomer] commands.
type RegisterCustomerHandler struct {
	repo *flux.AggregateRepository[*customer.CustomerAggregate, events.CustomerEvent]
}

// NewRegisterCustomerHandler creates a handler for [RegisterCustomer].
func NewRegisterCustomerHandler(repo *flux.AggregateRepository[*customer.CustomerAggregate, events.CustomerEvent]) *RegisterCustomerHandler {
	return &RegisterCustomerHandler{repo: repo}
}

// Handle executes the [RegisterCustomer] command.
func (h *RegisterCustomerHandler) Handle(ctx command.Context, cmd RegisterCustomer) error {
	stream := flux.Stream{Identifier: identity.NewCustomerIdentifier(cmd.CustomerID)}
	var zero *customer.CustomerAggregate
	c := zero.New(stream)

	if err := c.Register(cmd.Name, cmd.Email); err != nil {
		return fmt.Errorf("registering customer %s: %w", cmd.CustomerID, err)
	}

	if err := h.repo.Save(ctx, c); err != nil {
		return fmt.Errorf("saving customer %s: %w", cmd.CustomerID, err)
	}
	return nil
}
