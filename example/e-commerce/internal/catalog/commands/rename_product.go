package commands

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/aggregates/product"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/events"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
)

// RenameProduct represents an intent to update a product's name.
type RenameProduct struct {
	ProductID string `json:"product_id"`
	NewName   string `json:"new_name"`
}

// RenameProductHandler processes [RenameProduct] commands.
type RenameProductHandler struct {
	repo *flux.AggregateRepository[*product.ProductAggregate, events.ProductEvent]
}

// NewRenameProductHandler creates a handler for [RenameProduct].
func NewRenameProductHandler(repo *flux.AggregateRepository[*product.ProductAggregate, events.ProductEvent]) *RenameProductHandler {
	return &RenameProductHandler{repo: repo}
}

// Handle executes the [RenameProduct] command.
func (h *RenameProductHandler) Handle(ctx command.Context, cmd RenameProduct) error {
	stream := flux.Stream{Identifier: identity.NewProductIdentifier(cmd.ProductID)}
	p, err := h.repo.Load(ctx, stream)
	if err != nil {
		return fmt.Errorf("loading product %s: %w", cmd.ProductID, err)
	}

	if err := p.Rename(cmd.NewName); err != nil {
		return fmt.Errorf("renaming product %s: %w", cmd.ProductID, err)
	}

	if err := h.repo.Save(ctx, p); err != nil {
		return fmt.Errorf("saving product %s: %w", cmd.ProductID, err)
	}
	return nil
}
