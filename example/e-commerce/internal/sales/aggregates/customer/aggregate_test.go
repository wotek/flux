package customer_test

import (
	"errors"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
	"github.com/wotek/flux/example/e-commerce/internal/sales/aggregates/customer"
	"github.com/wotek/flux/example/e-commerce/internal/types"
)

func TestCustomerAggregate(t *testing.T) {
	t.Parallel()

	id := identity.NewCustomerIdentifier("cust-1")
	stream := flux.Stream{Identifier: id}

	t.Run("register customer success", func(t *testing.T) {
		t.Parallel()

		var zero *customer.CustomerAggregate
		c := zero.New(stream)

		if err := c.Register("Alice", "alice@example.com"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		changes := c.Changeset().Events()
		if len(changes) != 1 {
			t.Fatalf("expected 1 event recorded, got %d", len(changes))
		}

		if c.Name() != "Alice" {
			t.Errorf("expected name 'Alice', got %q", c.Name())
		}
		if c.Email() != "alice@example.com" {
			t.Errorf("expected email 'alice@example.com', got %q", c.Email())
		}
	})

	t.Run("address management", func(t *testing.T) {
		t.Parallel()

		var zero *customer.CustomerAggregate
		c := zero.New(stream)
		_ = c.Register("Alice", "alice@example.com")

		addr := types.Address{
			ID:      "addr-1",
			Street:  "123 Main St",
			City:    "Metropolis",
			ZipCode: "12345",
			Country: "US",
		}

		if err := c.AddAddress(addr); err != nil {
			t.Fatalf("unexpected error adding address: %v", err)
		}
		if len(c.Addresses()) != 1 {
			t.Fatalf("expected 1 address, got %d", len(c.Addresses()))
		}

		if err := c.AddAddress(addr); !errors.Is(err, customer.ErrDuplicateAddress) {
			t.Errorf("expected ErrDuplicateAddress, got %v", err)
		}

		if err := c.RemoveAddress("addr-1"); err != nil {
			t.Fatalf("unexpected error removing address: %v", err)
		}
		if len(c.Addresses()) != 0 {
			t.Fatalf("expected 0 addresses, got %d", len(c.Addresses()))
		}

		if err := c.RemoveAddress("addr-1"); !errors.Is(err, customer.ErrAddressNotFound) {
			t.Errorf("expected ErrAddressNotFound, got %v", err)
		}
	})
}
