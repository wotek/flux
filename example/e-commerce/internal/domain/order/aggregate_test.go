package order_test

import (
	"errors"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/e-commerce/internal/domain/order"
	"github.com/wotek/flux/example/e-commerce/internal/domain/types"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
)

func TestOrderAggregate(t *testing.T) {
	t.Parallel()

	id := identity.NewOrderIdentifier("ord-1")
	stream := flux.Stream{Identifier: id}

	items := []types.LineItem{
		{ProductID: "prod-1", Name: "Mouse", Price: 2500, Quantity: 2},
		{ProductID: "prod-2", Name: "Keyboard", Price: 7500, Quantity: 1},
	}

	t.Run("place order success", func(t *testing.T) {
		t.Parallel()

		var zero *order.OrderAggregate
		o := zero.New(stream)

		if err := o.Place("cust-1", items); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if o.CustomerID() != "cust-1" {
			t.Errorf("expected customer 'cust-1', got %q", o.CustomerID())
		}
		if len(o.Items()) != 2 {
			t.Errorf("expected 2 items, got %d", len(o.Items()))
		}
		if o.Status() != order.OrderStatusOpen {
			t.Errorf("expected status 'open', got %s", o.Status())
		}
	})

	t.Run("place order validation", func(t *testing.T) {
		t.Parallel()

		var zero *order.OrderAggregate
		o := zero.New(stream)

		if err := o.Place("", items); !errors.Is(err, order.ErrEmptyCustomerID) {
			t.Errorf("expected ErrEmptyCustomerID, got %v", err)
		}
		if err := o.Place("cust-1", nil); !errors.Is(err, order.ErrNoLineItems) {
			t.Errorf("expected ErrNoLineItems, got %v", err)
		}
		badItems := []types.LineItem{{ProductID: "p1", Name: "Item", Price: 10, Quantity: 0}}
		if err := o.Place("cust-1", badItems); !errors.Is(err, order.ErrInvalidItemQuantity) {
			t.Errorf("expected ErrInvalidItemQuantity, got %v", err)
		}
	})

	t.Run("pay order lifecycle", func(t *testing.T) {
		t.Parallel()

		var zero *order.OrderAggregate
		o := zero.New(stream)
		_ = o.Place("cust-1", items)

		if err := o.Pay(); err != nil {
			t.Fatalf("unexpected error paying: %v", err)
		}
		if o.Status() != order.OrderStatusPaid {
			t.Errorf("expected status 'paid', got %s", o.Status())
		}

		// Cannot cancel paid order
		if err := o.Cancel("customer request"); !errors.Is(err, order.ErrOrderNotOpen) {
			t.Errorf("expected ErrOrderNotOpen when cancelling paid order, got %v", err)
		}
	})

	t.Run("cancel order lifecycle", func(t *testing.T) {
		t.Parallel()

		var zero *order.OrderAggregate
		o := zero.New(stream)
		_ = o.Place("cust-1", items)

		if err := o.Cancel("out of stock"); err != nil {
			t.Fatalf("unexpected error cancelling: %v", err)
		}
		if o.Status() != order.OrderStatusCancelled {
			t.Errorf("expected status 'cancelled', got %s", o.Status())
		}

		// Cannot pay cancelled order
		if err := o.Pay(); !errors.Is(err, order.ErrOrderNotOpen) {
			t.Errorf("expected ErrOrderNotOpen when paying cancelled order, got %v", err)
		}
	})
}
