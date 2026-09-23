package customer

import (
	"errors"
	"fmt"
	"slices"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/e-commerce/internal/sales/events"
	"github.com/wotek/flux/example/e-commerce/internal/types"
)

var (
	// ErrEmptyCustomerName indicates that the customer name is empty.
	ErrEmptyCustomerName = errors.New("customer name cannot be empty")

	// ErrEmptyCustomerEmail indicates that the customer email is empty.
	ErrEmptyCustomerEmail = errors.New("customer email cannot be empty")

	// ErrEmptyAddressID indicates that the address ID is empty.
	ErrEmptyAddressID = errors.New("address id cannot be empty")

	// ErrDuplicateAddress indicates that an address with the given ID already exists.
	ErrDuplicateAddress = errors.New("address with this id already exists")

	// ErrAddressNotFound indicates that the address to remove was not found.
	ErrAddressNotFound = errors.New("address not found in customer address book")
)

var _ flux.Aggregate[*CustomerAggregate, events.CustomerEvent] = (*CustomerAggregate)(nil)

// CustomerAggregate manages customer profile details and the address book.
type CustomerAggregate struct {
	flux.AggregateRoot[events.CustomerEvent]
	name      string
	email     string
	addresses []types.Address
}

// New creates an uninitialized [CustomerAggregate] bound to the given stream.
func (c *CustomerAggregate) New(stream flux.Stream) *CustomerAggregate {
	newC := &CustomerAggregate{
		addresses: make([]types.Address, 0),
	}
	newC.AggregateRoot = flux.NewAggregateRoot[events.CustomerEvent](stream, flux.NewChangeset[events.CustomerEvent](), newC.apply)
	return newC
}

// Name returns the customer name.
func (c *CustomerAggregate) Name() string {
	return c.name
}

// Email returns the customer email address.
func (c *CustomerAggregate) Email() string {
	return c.email
}

// Addresses returns a copy of the customer's addresses.
func (c *CustomerAggregate) Addresses() []types.Address {
	return slices.Clone(c.addresses)
}

// Register validates customer details and records the [events.CustomerRegistered] event.
func (c *CustomerAggregate) Register(name, email string) error {
	if name == "" {
		return ErrEmptyCustomerName
	}
	if email == "" {
		return ErrEmptyCustomerEmail
	}
	c.record(events.CustomerRegistered{
		CustomerName: name,
		Email:        email,
	})
	return nil
}

// Rename validates input and records the [events.CustomerRenamed] event.
func (c *CustomerAggregate) Rename(name string) error {
	if name == "" {
		return ErrEmptyCustomerName
	}
	c.record(events.CustomerRenamed{
		CustomerName: name,
	})
	return nil
}

// AddAddress validates and appends a new address, recording [events.AddressAdded].
func (c *CustomerAggregate) AddAddress(addr types.Address) error {
	if addr.ID == "" {
		return ErrEmptyAddressID
	}
	if slices.ContainsFunc(c.addresses, func(a types.Address) bool { return a.ID == addr.ID }) {
		return ErrDuplicateAddress
	}
	c.record(events.AddressAdded{
		Address: addr,
	})
	return nil
}

// RemoveAddress removes an address by ID, recording [events.AddressRemoved].
func (c *CustomerAggregate) RemoveAddress(addressID string) error {
	if addressID == "" {
		return ErrEmptyAddressID
	}
	if !slices.ContainsFunc(c.addresses, func(a types.Address) bool { return a.ID == addressID }) {
		return ErrAddressNotFound
	}
	c.record(events.AddressRemoved{
		AddressID: addressID,
	})
	return nil
}

func (c *CustomerAggregate) record(evt events.CustomerEvent) {
	c.Changeset().Record(evt)
	_ = c.apply(evt)
}

func (c *CustomerAggregate) apply(evt events.CustomerEvent) error {
	switch e := evt.(type) {
	case events.CustomerRegistered:
		c.name = e.CustomerName
		c.email = e.Email
	case events.CustomerRenamed:
		c.name = e.CustomerName
	case events.AddressAdded:
		c.addresses = append(c.addresses, e.Address)
	case events.AddressRemoved:
		c.addresses = slices.DeleteFunc(c.addresses, func(a types.Address) bool {
			return a.ID == e.AddressID
		})
	default:
		return fmt.Errorf("unhandled customer event: %s", evt.Name())
	}
	return nil
}
