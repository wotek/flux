package events

import "github.com/wotek/flux"

// ProductEvent is the sealed marker interface for events emitted by ProductAggregate.
type ProductEvent interface {
	flux.Event
	isProductEvent()
}

// PricingEvent is the sealed marker interface for events emitted by PricingAggregate.
type PricingEvent interface {
	flux.Event
	isPricingEvent()
}
