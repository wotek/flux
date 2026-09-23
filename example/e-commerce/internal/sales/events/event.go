package events

import "github.com/wotek/flux"

// CustomerEvent is the sealed marker interface for events emitted by CustomerAggregate.
type CustomerEvent interface {
	flux.Event
	isCustomerEvent()
}

// OrderEvent is the sealed marker interface for events emitted by OrderAggregate.
type OrderEvent interface {
	flux.Event
	isOrderEvent()
}
