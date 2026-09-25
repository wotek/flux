package product

import (
	"errors"
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/e-commerce/internal/catalog/events"
)

var (
	// ErrEmptyProductName indicates that a product name was not provided.
	ErrEmptyProductName = errors.New("product name cannot be empty")

	// ErrNegativeInitialStock indicates that the initial stock level is negative.
	ErrNegativeInitialStock = errors.New("initial stock cannot be negative")

	// ErrInsufficientStock indicates an operation would reduce stock below zero.
	ErrInsufficientStock = errors.New("insufficient stock for adjustment")
)

var _ flux.Aggregate[*ProductAggregate, events.ProductEvent] = (*ProductAggregate)(nil)

// ProductAggregate manages the core identity, naming, and inventory of a product.
type ProductAggregate struct {
	flux.AggregateRoot[events.ProductEvent]
	name  string
	stock int
}

// New creates an uninitialized [ProductAggregate] bound to the given stream.
func (p *ProductAggregate) New(stream flux.Stream) *ProductAggregate {
	newP := &ProductAggregate{}
	newP.AggregateRoot = flux.NewAggregateRoot[events.ProductEvent](stream, flux.NewChangeset[events.ProductEvent](), newP.apply)
	return newP
}

// Name returns the product name.
func (p *ProductAggregate) Name() string {
	return p.name
}

// Stock returns the available product inventory count.
func (p *ProductAggregate) Stock() int {
	return p.stock
}

// Create validates input and records the [events.ProductCreated] event.
func (p *ProductAggregate) Create(name string, stock int) error {
	if name == "" {
		return ErrEmptyProductName
	}
	if stock < 0 {
		return ErrNegativeInitialStock
	}
	evt := events.ProductCreated{
		ProductName: name,
		Stock:       stock,
	}
	p.apply(evt)
	p.Changeset().Record(evt)
	return nil
}

// Rename validates input and records the [events.ProductRenamed] event.
func (p *ProductAggregate) Rename(name string) error {
	if name == "" {
		return ErrEmptyProductName
	}
	evt := events.ProductRenamed{
		ProductName: name,
	}
	p.apply(evt)
	p.Changeset().Record(evt)
	return nil
}

// AdjustStock validates inventory availability and records the [events.StockAdjusted] event.
func (p *ProductAggregate) AdjustStock(quantity int) error {
	if p.stock+quantity < 0 {
		return fmt.Errorf("%w: current %d, change %d", ErrInsufficientStock, p.stock, quantity)
	}
	evt := events.StockAdjusted{
		Quantity: quantity,
	}
	p.apply(evt)
	p.Changeset().Record(evt)
	return nil
}

func (p *ProductAggregate) apply(evt events.ProductEvent) {
	switch e := evt.(type) {
	case events.ProductCreated:
		p.name = e.ProductName
		p.stock = e.Stock
	case events.ProductRenamed:
		p.name = e.ProductName
	case events.StockAdjusted:
		p.stock += e.Quantity
	}
}
