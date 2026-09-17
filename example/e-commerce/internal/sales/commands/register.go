package commands

import (
	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/sales/aggregates/customer"
	"github.com/wotek/flux/example/e-commerce/internal/sales/aggregates/order"
	"github.com/wotek/flux/example/e-commerce/internal/sales/events"
)

// RegisterHandlers wires all sales command handlers to the provided command bus.
func RegisterHandlers(
	bus *command.Bus,
	customerRepo *flux.AggregateRepository[*customer.CustomerAggregate, events.CustomerEvent],
	orderRepo *flux.AggregateRepository[*order.OrderAggregate, events.OrderEvent],
) {
	command.RegisterHandler(bus, NewRegisterCustomerHandler(customerRepo))
	command.RegisterHandler(bus, NewRenameCustomerHandler(customerRepo))
	command.RegisterHandler(bus, NewAddAddressHandler(customerRepo))
	command.RegisterHandler(bus, NewRemoveAddressHandler(customerRepo))
	command.RegisterHandler(bus, NewPlaceOrderHandler(orderRepo))
	command.RegisterHandler(bus, NewPayOrderHandler(orderRepo))
	command.RegisterHandler(bus, NewCancelOrderHandler(orderRepo))
}
