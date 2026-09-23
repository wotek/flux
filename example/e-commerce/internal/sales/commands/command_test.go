package commands_test

import (
	"context"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	eventstore "github.com/wotek/flux/event/store"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
	"github.com/wotek/flux/example/e-commerce/internal/sales/aggregates/customer"
	"github.com/wotek/flux/example/e-commerce/internal/sales/aggregates/order"
	"github.com/wotek/flux/example/e-commerce/internal/sales/commands"
	"github.com/wotek/flux/example/e-commerce/internal/sales/events"
	"github.com/wotek/flux/example/e-commerce/internal/sales/types"
)

func TestSalesCommandDispatching(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	es := eventstore.New()
	cmdBus := command.New()

	customerRepo := flux.NewAggregateRepository[*customer.CustomerAggregate, events.CustomerEvent](es)
	orderRepo := flux.NewAggregateRepository[*order.OrderAggregate, events.OrderEvent](es)
	commands.RegisterHandlers(cmdBus, customerRepo, orderRepo)

	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:flux:ecommerce:shop:default:user:tester")}
	cmdCtx := command.NewContext(ctx, flux.MustParseIdentifier("urn:flux:ecommerce:shop:default:command:c1"), actor, flux.Identifier{}, flux.Identifier{})

	if err := command.Execute(cmdCtx, cmdBus, commands.RegisterCustomer{
		CustomerID: "cust-1",
		Name:       "Jane Doe",
		Email:      "jane@example.com",
	}); err != nil {
		t.Fatalf("RegisterCustomer failed: %v", err)
	}

	if err := command.Execute(cmdCtx, cmdBus, commands.PlaceOrder{
		OrderID:    "ord-1",
		CustomerID: "cust-1",
		Items: []types.LineItem{
			{ProductID: "prod-1", Name: "Monitor", Price: 34900, Quantity: 1},
		},
	}); err != nil {
		t.Fatalf("PlaceOrder failed: %v", err)
	}

	fluxCtx := flux.NewContext(ctx, actor, flux.Identifier{}, flux.Identifier{})
	ord, err := orderRepo.Load(fluxCtx, flux.Stream{Identifier: identity.NewOrderIdentifier("ord-1")})
	if err != nil {
		t.Fatalf("failed loading order: %v", err)
	}
	if ord.Status() != types.OrderStatusOpen {
		t.Errorf("unexpected status: %s", ord.Status())
	}
}
