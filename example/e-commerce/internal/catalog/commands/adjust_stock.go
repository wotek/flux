package commands

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/aggregates/product"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/events"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
)

// AdjustStock represents an intent to increment or decrement product inventory stock.
type AdjustStock struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

// AdjustStockHandler processes [AdjustStock] commands.
type AdjustStockHandler struct {
	repo *flux.AggregateRepository[*product.ProductAggregate, events.ProductEvent]
}

// NewAdjustStockHandler creates a handler for [AdjustStock].
func NewAdjustStockHandler(repo *flux.AggregateRepository[*product.ProductAggregate, events.ProductEvent]) *AdjustStockHandler {
	return &AdjustStockHandler{repo: repo}
}

// Handle executes the [AdjustStock] command.
func (h *AdjustStockHandler) Handle(ctx command.Context, cmd AdjustStock) error {
	stream := flux.Stream{Identifier: identity.NewProductIdentifier(cmd.ProductID)}
	p, err := h.repo.Load(ctx, stream)
	if err != nil {
		return fmt.Errorf("loading product %s: %w", cmd.ProductID, err)
	}

	if err := p.AdjustStock(cmd.Quantity); err != nil {
		return fmt.Errorf("adjusting stock for product %s: %w", cmd.ProductID, err)
	}

	if err := h.repo.Save(ctx, p); err != nil {
		return fmt.Errorf("saving product %s: %w", cmd.ProductID, err)
	}
	return nil
}
