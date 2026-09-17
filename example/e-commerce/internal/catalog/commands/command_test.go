package commands_test

import (
	"context"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	eventstore "github.com/wotek/flux/event/store"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/aggregates/pricing"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/aggregates/product"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/commands"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/events"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
)

func TestCatalogCommandDispatching(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	es := eventstore.New()
	cmdBus := command.New()

	productRepo := flux.NewAggregateRepository[*product.ProductAggregate, events.ProductEvent](es)
	pricingRepo := flux.NewAggregateRepository[*pricing.PricingAggregate, events.PricingEvent](es)
	commands.RegisterHandlers(cmdBus, productRepo, pricingRepo)

	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:user:tester")}
	cmdCtx := command.NewContext(ctx, flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:command:c1"), actor, flux.Identifier{}, flux.Identifier{})

	if err := command.Execute(cmdCtx, cmdBus, commands.CreateProduct{
		ProductID: "prod-1",
		Name:      "Monitor",
		Stock:     20,
	}); err != nil {
		t.Fatalf("CreateProduct failed: %v", err)
	}

	if err := command.Execute(cmdCtx, cmdBus, commands.SetPrice{
		ProductID: "prod-1",
		Price:     34900,
	}); err != nil {
		t.Fatalf("SetPrice failed: %v", err)
	}

	fluxCtx := flux.NewContext(ctx, actor, flux.Identifier{}, flux.Identifier{})
	prod, err := productRepo.Load(fluxCtx, flux.Stream{Identifier: identity.NewProductIdentifier("prod-1")})
	if err != nil {
		t.Fatalf("failed loading product: %v", err)
	}
	if prod.Name() != "Monitor" || prod.Stock() != 20 {
		t.Errorf("unexpected product state: name=%s stock=%d", prod.Name(), prod.Stock())
	}

	pr, err := pricingRepo.Load(fluxCtx, flux.Stream{Identifier: identity.NewPricingIdentifier("prod-1")})
	if err != nil {
		t.Fatalf("failed loading pricing: %v", err)
	}
	if pr.Price() != 34900 {
		t.Errorf("unexpected price: %d", pr.Price())
	}
}
