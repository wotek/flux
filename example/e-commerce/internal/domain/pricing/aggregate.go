package pricing

import (
	"errors"
	"fmt"

	"github.com/wotek/flux"
)

var (
	// ErrNegativePrice indicates that the price cannot be negative.
	ErrNegativePrice = errors.New("price cannot be negative")
)

// PricingAggregate manages pricing concerns exclusively, sharing the exact same ID as the product it prices.
type PricingAggregate struct {
	flux.AggregateRoot[PricingEvent]
	price int
}

// New creates an uninitialized [PricingAggregate] bound to the given stream.
func (p *PricingAggregate) New(stream flux.Stream) *PricingAggregate {
	newP := &PricingAggregate{}
	newP.AggregateRoot = flux.NewAggregateRoot[PricingEvent](stream, flux.NewChangeset[PricingEvent](), newP.apply)
	return newP
}

// Price returns the current price in cents.
func (p *PricingAggregate) Price() int {
	return p.price
}

// SetPrice validates input and records the [PricingSet] event.
func (p *PricingAggregate) SetPrice(price int) error {
	if price < 0 {
		return ErrNegativePrice
	}
	p.record(PricingSet{
		Price: price,
	})
	return nil
}

func (p *PricingAggregate) record(evt PricingEvent) {
	p.Changeset().Record(evt)
	_ = p.apply(evt)
}

func (p *PricingAggregate) apply(evt PricingEvent) error {
	switch e := evt.(type) {
	case PricingSet:
		p.price = e.Price
	default:
		return fmt.Errorf("unhandled pricing event: %s", evt.Name())
	}
	return nil
}
