package projections

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/events"
	"github.com/wotek/flux/projection"
)

// NewProductCatalogProjector creates and configures the catalog read-model projector.
func NewProductCatalogProjector(
	projID flux.Identifier,
	eventStore flux.EventStore,
	projStore projection.Store,
	catalogStore Store,
) *projection.Projector {
	p := projection.NewProjector(projID, eventStore, projStore)

	// 1. ProductCreated initializes the view
	projection.RegisterHandler(p, func(ctx projection.Context, evt events.ProductCreated) error {
		id := ctx.Stream().Identifier.ResourceID()
		view, _, err := catalogStore.Get(ctx, id)
		if err != nil {
			return fmt.Errorf("getting product view for %s: %w", id, err)
		}

		view.ID = id
		view.Name = evt.ProductName
		view.Stock = evt.Stock

		if err := catalogStore.Save(ctx, view); err != nil {
			return fmt.Errorf("saving product view for %s: %w", id, err)
		}
		return nil
	})

	// 2. ProductRenamed updates Name
	projection.RegisterHandler(p, func(ctx projection.Context, evt events.ProductRenamed) error {
		id := ctx.Stream().Identifier.ResourceID()
		view, exists, err := catalogStore.Get(ctx, id)
		if err != nil {
			return fmt.Errorf("getting product view for %s: %w", id, err)
		}
		if !exists {
			view.ID = id
		}

		view.Name = evt.ProductName

		if err := catalogStore.Save(ctx, view); err != nil {
			return fmt.Errorf("saving product view for %s: %w", id, err)
		}
		return nil
	})

	// 3. StockAdjusted updates Stock
	projection.RegisterHandler(p, func(ctx projection.Context, evt events.StockAdjusted) error {
		id := ctx.Stream().Identifier.ResourceID()
		view, exists, err := catalogStore.Get(ctx, id)
		if err != nil {
			return fmt.Errorf("getting product view for %s: %w", id, err)
		}
		if !exists {
			view.ID = id
		}

		view.Stock += evt.Quantity

		if err := catalogStore.Save(ctx, view); err != nil {
			return fmt.Errorf("saving product view for %s: %w", id, err)
		}
		return nil
	})

	// 4. PricingSet updates Price
	projection.RegisterHandler(p, func(ctx projection.Context, evt events.PricingSet) error {
		id := ctx.Stream().Identifier.ResourceID()
		view, exists, err := catalogStore.Get(ctx, id)
		if err != nil {
			return fmt.Errorf("getting product view for %s: %w", id, err)
		}
		if !exists {
			view.ID = id
		}

		view.Price = evt.Price

		if err := catalogStore.Save(ctx, view); err != nil {
			return fmt.Errorf("saving product view for %s: %w", id, err)
		}
		return nil
	})

	return p
}
