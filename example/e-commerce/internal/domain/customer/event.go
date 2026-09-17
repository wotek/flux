package customer

import (
	"github.com/wotek/flux"
)

// CustomerEvent is a marker interface for all customer domain events.
type CustomerEvent interface {
	flux.Event
	isCustomerEvent()
}
