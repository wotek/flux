package projections_test

import (
	"context"
	"testing"
	"time"

	"github.com/wotek/flux"
	eventstore "github.com/wotek/flux/event/store"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/events"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/projections"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
	projectionstore "github.com/wotek/flux/projection/store"
)

func TestCatalogProjector_MergesProductAndPricing(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	es := eventstore.New()
	ps := projectionstore.New()
	store := projections.NewMemoryStore()

	projectorID := flux.MustParseIdentifier("urn:flux:ecommerce:shop:default:projection:catalog")
	proj := projections.NewProductCatalogProjector(projectorID, es, ps, store)

	go func() {
		_ = proj.Start(ctx)
	}()

	prodUUID := "item-42"
	actor := flux.Actor{Identifier: flux.MustParseIdentifier("urn:flux:ecommerce:shop:default:user:test")}

	prodStream := flux.Stream{Identifier: identity.NewProductIdentifier(prodUUID)}
	_ = es.Append(ctx, prodStream, 0, []flux.Envelope{
		{
			Identifier: flux.MustParseIdentifier("urn:flux:ecommerce:shop:default:event:e1"),
			Stream:     prodStream,
			Revision:   1,
			Event: events.ProductCreated{
				ProductName: "Ergonomic Chair",
				Stock:       10,
			},
			Actor:     actor,
			CreatedAt: time.Now(),
		},
	})

	pricingStream := flux.Stream{Identifier: identity.NewPricingIdentifier(prodUUID)}
	_ = es.Append(ctx, pricingStream, 0, []flux.Envelope{
		{
			Identifier: flux.MustParseIdentifier("urn:flux:ecommerce:shop:default:event:e2"),
			Stream:     pricingStream,
			Revision:   1,
			Event: events.PricingSet{
				Price: 19999,
			},
			Actor:     actor,
			CreatedAt: time.Now(),
		},
	})

	_ = es.Append(ctx, prodStream, 1, []flux.Envelope{
		{
			Identifier: flux.MustParseIdentifier("urn:flux:ecommerce:shop:default:event:e3"),
			Stream:     prodStream,
			Revision:   2,
			Event: events.StockAdjusted{
				Quantity: -2,
			},
			Actor:     actor,
			CreatedAt: time.Now(),
		},
	})

	deadline := time.Now().Add(2 * time.Second)
	var foundView projections.ProductView
	var found bool
	for time.Now().Before(deadline) {
		view, exists, err := store.Get(ctx, prodUUID)
		if err == nil && exists && view.Price == 19999 && view.Stock == 8 {
			foundView = view
			found = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !found {
		t.Fatalf("timed out waiting for projector to build combined ProductView")
	}

	if foundView.ID != prodUUID {
		t.Errorf("expected ID %s, got %s", prodUUID, foundView.ID)
	}
	if foundView.Name != "Ergonomic Chair" {
		t.Errorf("expected Name 'Ergonomic Chair', got %s", foundView.Name)
	}
	if foundView.Price != 19999 {
		t.Errorf("expected Price 19999, got %d", foundView.Price)
	}
	if foundView.Stock != 8 {
		t.Errorf("expected Stock 8, got %d", foundView.Stock)
	}
}
