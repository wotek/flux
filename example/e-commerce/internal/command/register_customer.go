package command

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/domain/customer"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
)

// RegisterCustomer is a command instructing the system to register a new customer profile.
type RegisterCustomer struct {
	CustomerID string `json:"customer_id"`
	Name       string `json:"name"`
	Email      string `json:"email"`
}

// RegisterCustomerHandler executes the [RegisterCustomer] command against the customer repository.
type RegisterCustomerHandler struct {
	repo *flux.AggregateRepository[*customer.CustomerAggregate, customer.CustomerEvent]
}

// NewRegisterCustomerHandler creates a new [RegisterCustomerHandler].
func NewRegisterCustomerHandler(repo *flux.AggregateRepository[*customer.CustomerAggregate, customer.CustomerEvent]) *RegisterCustomerHandler {
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
