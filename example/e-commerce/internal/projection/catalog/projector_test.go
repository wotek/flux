package catalog_test

import (
	"context"
	"testing"
	"time"

	"github.com/wotek/flux"
	eventstore "github.com/wotek/flux/event/store"
	"github.com/wotek/flux/example/e-commerce/internal/domain/pricing"
	"github.com/wotek/flux/example/e-commerce/internal/domain/product"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
	"github.com/wotek/flux/example/e-commerce/internal/projection/catalog"
	projectionstore "github.com/wotek/flux/projection/store"
)

func TestCatalogProjector_MergesProductAndPricing(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	es := eventstore.New()
	ps := projectionstore.New()
	catStore := catalog.NewMemoryCatalogStore()

	projID := flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:projection:catalog")
	projector := catalog.NewCatalogProjector(projID, es, ps, catStore)

	go func() {
		_ = projector.Start(ctx)
	}()

	actor := flux.Actor{Identifier: flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:user:test")}
	prodID := "item-42"

	// 1. Emit ProductCreated on product stream
	prodStream := flux.Stream{Identifier: identity.NewProductIdentifier(prodID)}
	_ = es.Append(ctx, prodStream, 0, []flux.Envelope{
		{
			Identifier:            flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:event:e1"),
			Stream:                prodStream,
			Revision:              1,
			Event:                 product.ProductCreated{ProductName: "Initial Name", Stock: 100},
			Actor:                 actor,
			CorrelationIdentifier: flux.Identifier{},
			CreatedAt:             time.Now(),
		},
	})

	// 2. Emit PricingSet on pricing stream (shared UUID!)
	pricingStream := flux.Stream{Identifier: identity.NewPricingIdentifier(prodID)}
	_ = es.Append(ctx, pricingStream, 0, []flux.Envelope{
		{
			Identifier:            flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:event:e2"),
			Stream:                pricingStream,
			Revision:              1,
			Event:                 pricing.PricingSet{Price: 4999},
			Actor:                 actor,
			CorrelationIdentifier: flux.Identifier{},
			CreatedAt:             time.Now(),
		},
	})

	// 3. Emit ProductRenamed and StockAdjusted
	_ = es.Append(ctx, prodStream, 1, []flux.Envelope{
		{
			Identifier:            flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:event:e3"),
			Stream:                prodStream,
			Revision:              2,
			Event:                 product.ProductRenamed{ProductName: "Updated Gaming Mouse"},
			Actor:                 actor,
			CorrelationIdentifier: flux.Identifier{},
			CreatedAt:             time.Now(),
		},
		{
			Identifier:            flux.NewIdentifierFromString("urn:flux:ecommerce:shop:default:event:e4"),
			Stream:                prodStream,
			Revision:              3,
			Event:                 product.StockAdjusted{Quantity: -10},
			Actor:                 actor,
			CorrelationIdentifier: flux.Identifier{},
			CreatedAt:             time.Now(),
		},
	})

	// 4. Await projection convergence
	deadline := time.Now().Add(2 * time.Second)
	var view catalog.ProductView
	var found bool
	for time.Now().Before(deadline) {
		var err error
		view, found, err = catStore.Get(ctx, prodID)
		if err == nil && found && view.Name == "Updated Gaming Mouse" && view.Price == 4999 && view.Stock == 90 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !found {
		t.Fatalf("expected product view %s to be created", prodID)
	}
	if view.Name != "Updated Gaming Mouse" {
		t.Errorf("expected name 'Updated Gaming Mouse', got %q", view.Name)
	}
	if view.Price != 4999 {
		t.Errorf("expected price 4999, got %d", view.Price)
	}
	if view.Stock != 90 {
		t.Errorf("expected stock 90, got %d", view.Stock)
	}
}
