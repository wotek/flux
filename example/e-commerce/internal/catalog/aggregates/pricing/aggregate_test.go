package pricing_test

import (
	"errors"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/aggregates/pricing"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
)

func TestPricingAggregate(t *testing.T) {
	t.Parallel()

	id := identity.NewPricingIdentifier("prod-1")
	stream := flux.Stream{Identifier: id}

	t.Run("set price success", func(t *testing.T) {
		t.Parallel()

		var zero *pricing.PricingAggregate
		p := zero.New(stream)

		if err := p.SetPrice(2999); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		changes := p.Changeset().Events()
		if len(changes) != 1 {
			t.Fatalf("expected 1 event recorded, got %d", len(changes))
		}

		if p.Price() != 2999 {
			t.Errorf("expected price 2999, got %d", p.Price())
		}
	})

	t.Run("set price negative validation", func(t *testing.T) {
		t.Parallel()

		var zero *pricing.PricingAggregate
		p := zero.New(stream)

		if err := p.SetPrice(-100); !errors.Is(err, pricing.ErrNegativePrice) {
			t.Errorf("expected ErrNegativePrice, got %v", err)
		}
	})
}
