package commands

import (
	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/aggregates/pricing"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/aggregates/product"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/events"
)

// RegisterHandlers wires all catalog command handlers to the provided command bus.
func RegisterHandlers(
	bus *command.Bus,
	productRepo *flux.AggregateRepository[*product.ProductAggregate, events.ProductEvent],
	pricingRepo *flux.AggregateRepository[*pricing.PricingAggregate, events.PricingEvent],
) {
	command.RegisterHandler(bus, NewCreateProductHandler(productRepo))
	command.RegisterHandler(bus, NewRenameProductHandler(productRepo))
	command.RegisterHandler(bus, NewAdjustStockHandler(productRepo))
	command.RegisterHandler(bus, NewSetPriceHandler(pricingRepo))
}
