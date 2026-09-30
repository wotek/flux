package events

import "github.com/wotek/flux"

// OrderEvent is the sealed marker interface for events emitted by the order aggregate.
type OrderEvent interface {
	flux.Event
	isOrderEvent()
}
