package command_test

import (
	"context"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	eventstore "github.com/wotek/flux/event/store"
	ecommercecmd "github.com/wotek/flux/example/e-commerce/internal/command"
	"github.com/wotek/flux/example/e-commerce/internal/domain/customer"
	"github.com/wotek/flux/example/e-commerce/internal/domain/order"
	"github.com/wotek/flux/example/e-commerce/internal/domain/pricing"
	"github.com/wotek/flux/example/e-commerce/internal/domain/product"
	"github.com/wotek/flux/example/e-commerce/internal/domain/types"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
)

func TestCommandDispatching(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cmdBus := command.New()
	es := eventstore.New()

	productRepo := flux.NewAggregateRepository[*product.ProductAggregate, product.ProductEvent](es)
	pricingRepo := flux.NewAggregateRepository[*pricing.PricingAggregate, pricing.PricingEvent](es)
	customerRepo := flux.NewAggregateRepository[*customer.CustomerAggregate, customer.CustomerEvent](es)
	orderRepo := flux.NewAggregateRepository[*order.OrderAggregate, order.OrderEvent](es)

	ecommercecmd.RegisterHandlers(cmdBus, productRepo, pricingRepo, customerRepo, orderRepo)

	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:user:test")}

	// 1. Create and price product
	pID := "prod-100"
	cmdCtx := command.NewContext(ctx, flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:command:c1"), actor, identity.NewProductIdentifier(pID), flux.Identifier{})

	err := command.Execute(cmdCtx, cmdBus, ecommercecmd.CreateProduct{
		ProductID: pID,
		Name:      "Monitor",
		Stock:     10,
	})
	if err != nil {
		t.Fatalf("CreateProduct failed: %v", err)
	}

	err = command.Execute(cmdCtx, cmdBus, ecommercecmd.SetPrice{
		ProductID: pID,
		Price:     19999,
	})
	if err != nil {
		t.Fatalf("SetPrice failed: %v", err)
	}

	// 2. Register customer
	cID := "cust-100"
	cCmdCtx := command.NewContext(ctx, flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:command:c2"), actor, identity.NewCustomerIdentifier(cID), flux.Identifier{})

	err = command.Execute(cCmdCtx, cmdBus, ecommercecmd.RegisterCustomer{
		CustomerID: cID,
		Name:       "Charlie",
		Email:      "charlie@example.com",
	})
	if err != nil {
		t.Fatalf("RegisterCustomer failed: %v", err)
	}

	// 3. Place order
	oID := "ord-100"
	oCmdCtx := command.NewContext(ctx, flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:command:c3"), actor, identity.NewOrderIdentifier(oID), flux.Identifier{})

	err = command.Execute(oCmdCtx, cmdBus, ecommercecmd.PlaceOrder{
		OrderID:    oID,
		CustomerID: cID,
		Items: []types.LineItem{
			{ProductID: pID, Name: "Monitor", Price: 19999, Quantity: 1},
		},
	})
	if err != nil {
		t.Fatalf("PlaceOrder failed: %v", err)
	}

	// Verify order was placed in repository
	stream := flux.Stream{Identifier: identity.NewOrderIdentifier(oID)}
	ord, err := orderRepo.Load(oCmdCtx, stream)
	if err != nil {
		t.Fatalf("failed to load order: %v", err)
	}
	if ord.Status() != order.OrderStatusOpen {
		t.Errorf("expected order status open, got %s", ord.Status())
	}
}
