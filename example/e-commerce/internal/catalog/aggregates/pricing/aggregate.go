package pricing

import (
	"errors"
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/events"
)

var (
	// ErrNegativePrice indicates that the price cannot be negative.
	ErrNegativePrice = errors.New("price cannot be negative")
)

var _ flux.Aggregate[*PricingAggregate, events.PricingEvent] = (*PricingAggregate)(nil)

// PricingAggregate manages pricing concerns exclusively, sharing the exact same ID as the product it prices.
type PricingAggregate struct {
	flux.AggregateRoot[events.PricingEvent]
	price int
}

// New creates an uninitialized [PricingAggregate] bound to the given stream.
func (p *PricingAggregate) New(stream flux.Stream) *PricingAggregate {
	newP := &PricingAggregate{}
	newP.AggregateRoot = flux.NewAggregateRoot[events.PricingEvent](stream, flux.NewChangeset[events.PricingEvent](), newP.apply)
	return newP
}

// Price returns the current price in cents.
func (p *PricingAggregate) Price() int {
	return p.price
}

// SetPrice validates input and records the [events.PricingSet] event.
func (p *PricingAggregate) SetPrice(price int) error {
	if price < 0 {
		return ErrNegativePrice
	}
	p.record(events.PricingSet{
		Price: price,
	})
	return nil
}

func (p *PricingAggregate) record(evt events.PricingEvent) {
	p.Changeset().Record(evt)
	_ = p.apply(evt)
}

func (p *PricingAggregate) apply(evt events.PricingEvent) error {
	switch e := evt.(type) {
	case events.PricingSet:
		p.price = e.Price
	default:
		return fmt.Errorf("unhandled pricing event: %s", evt.Name())
	}
	return nil
}
