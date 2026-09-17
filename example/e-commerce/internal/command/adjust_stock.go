package command

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/domain/product"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
)

// AdjustStock is a command instructing the system to alter product inventory.
type AdjustStock struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

// AdjustStockHandler executes the [AdjustStock] command against the aggregate repository.
type AdjustStockHandler struct {
	repo *flux.AggregateRepository[*product.ProductAggregate, product.ProductEvent]
}

// NewAdjustStockHandler creates a new [AdjustStockHandler].
func NewAdjustStockHandler(repo *flux.AggregateRepository[*product.ProductAggregate, product.ProductEvent]) *AdjustStockHandler {
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
