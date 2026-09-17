package pricing

import (
	"github.com/wotek/flux"
)

// PricingEvent is a marker interface for all pricing domain events.
type PricingEvent interface {
	flux.Event
	isPricingEvent()
}
