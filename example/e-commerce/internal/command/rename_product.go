package command

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/domain/product"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
)

// RenameProduct is a command instructing the system to rename an existing product.
type RenameProduct struct {
	ProductID string `json:"product_id"`
	Name      string `json:"name"`
}

// RenameProductHandler executes the [RenameProduct] command against the aggregate repository.
type RenameProductHandler struct {
	repo *flux.AggregateRepository[*product.ProductAggregate, product.ProductEvent]
}

// NewRenameProductHandler creates a new [RenameProductHandler].
func NewRenameProductHandler(repo *flux.AggregateRepository[*product.ProductAggregate, product.ProductEvent]) *RenameProductHandler {
	return &RenameProductHandler{repo: repo}
}

// Handle executes the [RenameProduct] command.
func (h *RenameProductHandler) Handle(ctx command.Context, cmd RenameProduct) error {
	stream := flux.Stream{Identifier: identity.NewProductIdentifier(cmd.ProductID)}
	p, err := h.repo.Load(ctx, stream)
	if err != nil {
		return fmt.Errorf("loading product %s: %w", cmd.ProductID, err)
	}

	if err := p.Rename(cmd.Name); err != nil {
		return fmt.Errorf("renaming product %s: %w", cmd.ProductID, err)
	}

	if err := h.repo.Save(ctx, p); err != nil {
		return fmt.Errorf("saving product %s: %w", cmd.ProductID, err)
	}
	return nil
}
