package identity

import (
	"github.com/wotek/flux"
)

const (
	// Organization defines the top-level organization namespace.
	Organization = "flux"

	// Environment defines the operational environment namespace.
	Environment = "ecommerce"

	// Service defines the domain service namespace.
	Service = "shop"

	// AccountID defines the default tenant or account identifier.
	AccountID = "default"

	// ResourceTypeProduct defines the resource type for product aggregates.
	ResourceTypeProduct = "product"

	// ResourceTypePricing defines the resource type for pricing aggregates.
	ResourceTypePricing = "pricing"

	// ResourceTypeCustomer defines the resource type for customer aggregates.
	ResourceTypeCustomer = "customer"

	// ResourceTypeOrder defines the resource type for order aggregates.
	ResourceTypeOrder = "order"
)

// NewProductIdentifier constructs a deterministic URN identifier for a product aggregate.
func NewProductIdentifier(id string) flux.Identifier {
	return flux.NewIdentifier(Organization, Environment, Service, AccountID, ResourceTypeProduct, id, "")
}

// NewPricingIdentifier constructs a deterministic URN identifier for a pricing aggregate.
func NewPricingIdentifier(id string) flux.Identifier {
	return flux.NewIdentifier(Organization, Environment, Service, AccountID, ResourceTypePricing, id, "")
}

// NewCustomerIdentifier constructs a deterministic URN identifier for a customer aggregate.
func NewCustomerIdentifier(id string) flux.Identifier {
	return flux.NewIdentifier(Organization, Environment, Service, AccountID, ResourceTypeCustomer, id, "")
}

// NewOrderIdentifier constructs a deterministic URN identifier for an order aggregate.
func NewOrderIdentifier(id string) flux.Identifier {
	return flux.NewIdentifier(Organization, Environment, Service, AccountID, ResourceTypeOrder, id, "")
}
