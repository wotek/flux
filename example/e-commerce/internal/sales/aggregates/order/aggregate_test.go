package order_test

import (
	"errors"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
	"github.com/wotek/flux/example/e-commerce/internal/sales/aggregates/order"
	"github.com/wotek/flux/example/e-commerce/internal/sales/types"
)

func TestOrderAggregate(t *testing.T) {
	t.Parallel()

	id := identity.NewOrderIdentifier("ord-1")
	stream := flux.Stream{Identifier: id}

	items := []types.LineItem{
		{ProductID: "prod-1", Name: "Laptop", Price: 150000, Quantity: 1},
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
		if o.Status() != types.OrderStatusOpen {
			t.Errorf("expected status open, got %q", o.Status())
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
		invalidItems := []types.LineItem{{ProductID: "p1", Quantity: 0}}
		if err := o.Place("cust-1", invalidItems); !errors.Is(err, order.ErrInvalidItemQuantity) {
			t.Errorf("expected ErrInvalidItemQuantity, got %v", err)
		}
	})

	t.Run("pay order lifecycle", func(t *testing.T) {
		t.Parallel()

		var zero *order.OrderAggregate
		o := zero.New(stream)
		_ = o.Place("cust-1", items)

		if err := o.Pay(); err != nil {
			t.Fatalf("pay failed: %v", err)
		}
		if o.Status() != types.OrderStatusPaid {
			t.Errorf("expected status paid, got %q", o.Status())
		}

		if err := o.Pay(); !errors.Is(err, order.ErrOrderNotOpen) {
			t.Errorf("expected ErrOrderNotOpen when paying paid order, got %v", err)
		}
	})

	t.Run("cancel order lifecycle", func(t *testing.T) {
		t.Parallel()

		var zero *order.OrderAggregate
		o := zero.New(stream)
		_ = o.Place("cust-1", items)

		if err := o.Cancel("customer changed mind"); err != nil {
			t.Fatalf("cancel failed: %v", err)
		}
		if o.Status() != types.OrderStatusCancelled {
			t.Errorf("expected status cancelled, got %q", o.Status())
		}

		if err := o.Cancel("again"); !errors.Is(err, order.ErrOrderNotOpen) {
			t.Errorf("expected ErrOrderNotOpen when cancelling cancelled order, got %v", err)
		}
	})
}
