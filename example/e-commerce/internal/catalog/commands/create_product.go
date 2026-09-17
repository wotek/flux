package commands

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/aggregates/product"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/events"
	"github.com/wotek/flux/example/e-commerce/internal/identity"
)

// CreateProduct represents an intent to add a new product to the catalog.
type CreateProduct struct {
	ProductID string `json:"product_id"`
	Name      string `json:"name"`
	Stock     int    `json:"stock"`
}

// CreateProductHandler processes [CreateProduct] commands.
type CreateProductHandler struct {
	repo *flux.AggregateRepository[*product.ProductAggregate, events.ProductEvent]
}

// NewCreateProductHandler creates a handler for [CreateProduct].
func NewCreateProductHandler(repo *flux.AggregateRepository[*product.ProductAggregate, events.ProductEvent]) *CreateProductHandler {
	return &CreateProductHandler{repo: repo}
}

// Handle executes the [CreateProduct] command.
func (h *CreateProductHandler) Handle(ctx command.Context, cmd CreateProduct) error {
	stream := flux.Stream{Identifier: identity.NewProductIdentifier(cmd.ProductID)}
	var zero *product.ProductAggregate
	p := zero.New(stream)

	if err := p.Create(cmd.Name, cmd.Stock); err != nil {
		return fmt.Errorf("creating product %s: %w", cmd.ProductID, err)
	}

	if err := h.repo.Save(ctx, p); err != nil {
		return fmt.Errorf("saving product %s: %w", cmd.ProductID, err)
	}
	return nil
}
