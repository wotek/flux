package command

import (
	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/domain/customer"
	"github.com/wotek/flux/example/e-commerce/internal/domain/order"
	"github.com/wotek/flux/example/e-commerce/internal/domain/pricing"
	"github.com/wotek/flux/example/e-commerce/internal/domain/product"
)

// RegisterHandlers registers all domain command handlers with the provided command bus.
func RegisterHandlers(
	bus *command.Bus,
	productRepo *flux.AggregateRepository[*product.ProductAggregate, product.ProductEvent],
	pricingRepo *flux.AggregateRepository[*pricing.PricingAggregate, pricing.PricingEvent],
	customerRepo *flux.AggregateRepository[*customer.CustomerAggregate, customer.CustomerEvent],
	orderRepo *flux.AggregateRepository[*order.OrderAggregate, order.OrderEvent],
) {
	command.RegisterHandler(bus, NewCreateProductHandler(productRepo))
	command.RegisterHandler(bus, NewRenameProductHandler(productRepo))
	command.RegisterHandler(bus, NewAdjustStockHandler(productRepo))

	command.RegisterHandler(bus, NewSetPriceHandler(pricingRepo))

	command.RegisterHandler(bus, NewRegisterCustomerHandler(customerRepo))
	command.RegisterHandler(bus, NewRenameCustomerHandler(customerRepo))
	command.RegisterHandler(bus, NewAddAddressHandler(customerRepo))
	command.RegisterHandler(bus, NewRemoveAddressHandler(customerRepo))

	command.RegisterHandler(bus, NewPlaceOrderHandler(orderRepo))
	command.RegisterHandler(bus, NewPayOrderHandler(orderRepo))
	command.RegisterHandler(bus, NewCancelOrderHandler(orderRepo))
}
