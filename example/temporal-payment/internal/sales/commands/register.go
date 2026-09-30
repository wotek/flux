package commands

import (
	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/aggregates/order"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/events"
)

// Register wires sales command handlers onto the bus.
func Register(bus *command.Bus, repo *flux.AggregateRepository[*order.OrderAggregate, events.OrderEvent]) {
	command.RegisterHandler(bus, NewPlaceOrderHandler(repo))
	command.RegisterHandler(bus, NewPayOrderHandler(repo))
	command.RegisterHandler(bus, NewCancelOrderHandler(repo))
}
