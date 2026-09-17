package customer_test

import (
	"errors"
	"testing"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/e-commerce/internal/domain/customer"
	"github.com/wotek/flux/example/e-commerce/internal/domain/types"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
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

		if c.Name() != "Alice" {
			t.Errorf("expected 'Alice', got %q", c.Name())
		}
		if c.Email() != "alice@example.com" {
			t.Errorf("expected 'alice@example.com', got %q", c.Email())
		}
	})

	t.Run("address management", func(t *testing.T) {
		t.Parallel()

		var zero *customer.CustomerAggregate
		c := zero.New(stream)
		_ = c.Register("Bob", "bob@example.com")

		addr1 := types.Address{ID: "addr-1", Street: "123 Main St", City: "Springfield", ZipCode: "12345", Country: "USA"}
		addr2 := types.Address{ID: "addr-2", Street: "456 Elm St", City: "Shelbyville", ZipCode: "54321", Country: "USA"}

		if err := c.AddAddress(addr1); err != nil {
			t.Fatalf("failed to add addr1: %v", err)
		}
		if err := c.AddAddress(addr2); err != nil {
			t.Fatalf("failed to add addr2: %v", err)
		}

		if len(c.Addresses()) != 2 {
			t.Errorf("expected 2 addresses, got %d", len(c.Addresses()))
		}

		// Duplicate address ID test
		if err := c.AddAddress(addr1); !errors.Is(err, customer.ErrDuplicateAddress) {
			t.Errorf("expected ErrDuplicateAddress, got %v", err)
		}

		// Remove address
		if err := c.RemoveAddress("addr-1"); err != nil {
			t.Fatalf("failed to remove addr-1: %v", err)
		}

		if len(c.Addresses()) != 1 {
			t.Fatalf("expected 1 address, got %d", len(c.Addresses()))
		}
		if c.Addresses()[0].ID != "addr-2" {
			t.Errorf("expected remaining address to be addr-2, got %s", c.Addresses()[0].ID)
		}

		// Remove non-existent address
		if err := c.RemoveAddress("non-existent"); !errors.Is(err, customer.ErrAddressNotFound) {
			t.Errorf("expected ErrAddressNotFound, got %v", err)
		}
	})
}
