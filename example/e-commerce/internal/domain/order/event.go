package order

import (
	"github.com/wotek/flux"
)

// OrderEvent is a marker interface for all order domain events.
type OrderEvent interface {
	flux.Event
	isOrderEvent()
}
