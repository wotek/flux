package commands

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/aggregates/pricing"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/events"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
)

// SetPrice represents an intent to set or update a product's price in cents.
type SetPrice struct {
	ProductID string `json:"product_id"`
	Price     int    `json:"price"`
}

// SetPriceHandler processes [SetPrice] commands.
type SetPriceHandler struct {
	repo *flux.AggregateRepository[*pricing.PricingAggregate, events.PricingEvent]
}

// NewSetPriceHandler creates a handler for [SetPrice].
func NewSetPriceHandler(repo *flux.AggregateRepository[*pricing.PricingAggregate, events.PricingEvent]) *SetPriceHandler {
	return &SetPriceHandler{repo: repo}
}

// Handle executes the [SetPrice] command.
func (h *SetPriceHandler) Handle(ctx command.Context, cmd SetPrice) error {
	stream := flux.Stream{Identifier: identity.NewPricingIdentifier(cmd.ProductID)}
	p, err := h.repo.Load(ctx, stream)
	if err != nil {
		var zero *pricing.PricingAggregate
		p = zero.New(stream)
	}

	if err := p.SetPrice(cmd.Price); err != nil {
		return fmt.Errorf("setting price for product %s: %w", cmd.ProductID, err)
	}

	if err := h.repo.Save(ctx, p); err != nil {
		return fmt.Errorf("saving pricing for product %s: %w", cmd.ProductID, err)
	}
	return nil
}
