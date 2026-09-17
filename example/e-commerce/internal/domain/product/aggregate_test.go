package product_test

import (
	"errors"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/e-commerce/internal/domain/product"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
)

func TestProductAggregate(t *testing.T) {
	t.Parallel()

	id := identity.NewProductIdentifier("prod-1")
	stream := flux.Stream{Identifier: id}

	t.Run("create product success", func(t *testing.T) {
		t.Parallel()

		var zero *product.ProductAggregate
		p := zero.New(stream)

		if err := p.Create("Keyboard", 50); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		changes := p.Changeset().Events()
		if len(changes) != 1 {
			t.Fatalf("expected 1 event recorded, got %d", len(changes))
		}

		if p.Name() != "Keyboard" {
			t.Errorf("expected name 'Keyboard', got %q", p.Name())
		}
		if p.Stock() != 50 {
			t.Errorf("expected stock 50, got %d", p.Stock())
		}
	})

	t.Run("create product validation", func(t *testing.T) {
		t.Parallel()

		var zero *product.ProductAggregate
		p := zero.New(stream)

		if err := p.Create("", 10); !errors.Is(err, product.ErrEmptyProductName) {
			t.Errorf("expected ErrEmptyProductName, got %v", err)
		}
		if err := p.Create("Mouse", -1); !errors.Is(err, product.ErrNegativeInitialStock) {
			t.Errorf("expected ErrNegativeInitialStock, got %v", err)
		}
	})

	t.Run("rename and adjust stock", func(t *testing.T) {
		t.Parallel()

		var zero *product.ProductAggregate
		p := zero.New(stream)
		_ = p.Create("Old Name", 20)

		if err := p.Rename("New Name"); err != nil {
			t.Fatalf("rename failed: %v", err)
		}
		if err := p.AdjustStock(-5); err != nil {
			t.Fatalf("adjust stock failed: %v", err)
		}

		if p.Name() != "New Name" {
			t.Errorf("expected 'New Name', got %q", p.Name())
		}
		if p.Stock() != 15 {
			t.Errorf("expected stock 15, got %d", p.Stock())
		}

		// Insufficient stock test
		if err := p.AdjustStock(-20); !errors.Is(err, product.ErrInsufficientStock) {
			t.Errorf("expected ErrInsufficientStock, got %v", err)
		}
	})
}
