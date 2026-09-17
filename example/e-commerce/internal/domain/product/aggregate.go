package product

import (
	"errors"
	"fmt"

	"github.com/wotek/flux"
)

var (
	// ErrEmptyProductName indicates that a product name was not provided.
	ErrEmptyProductName = errors.New("product name cannot be empty")

	// ErrNegativeInitialStock indicates that the initial stock level is negative.
	ErrNegativeInitialStock = errors.New("initial stock cannot be negative")

	// ErrInsufficientStock indicates an operation would reduce stock below zero.
	ErrInsufficientStock = errors.New("insufficient stock for adjustment")
)

// ProductAggregate manages the core identity, naming, and inventory of a product.
type ProductAggregate struct {
	flux.AggregateRoot[ProductEvent]
	name  string
	stock int
}

// New creates an uninitialized [ProductAggregate] bound to the given stream.
func (p *ProductAggregate) New(stream flux.Stream) *ProductAggregate {
	newP := &ProductAggregate{}
	newP.AggregateRoot = flux.NewAggregateRoot[ProductEvent](stream, flux.NewChangeset[ProductEvent](), newP.apply)
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

// Create validates input and records the [ProductCreated] event.
func (p *ProductAggregate) Create(name string, stock int) error {
	if name == "" {
		return ErrEmptyProductName
	}
	if stock < 0 {
		return ErrNegativeInitialStock
	}
	p.record(ProductCreated{
		ProductName: name,
		Stock:       stock,
	})
	return nil
}

// Rename validates input and records the [ProductRenamed] event.
func (p *ProductAggregate) Rename(name string) error {
	if name == "" {
		return ErrEmptyProductName
	}
	p.record(ProductRenamed{
		ProductName: name,
	})
	return nil
}

// AdjustStock validates inventory availability and records the [StockAdjusted] event.
func (p *ProductAggregate) AdjustStock(quantity int) error {
	if p.stock+quantity < 0 {
		return fmt.Errorf("%w: current %d, change %d", ErrInsufficientStock, p.stock, quantity)
	}
	p.record(StockAdjusted{
		Quantity: quantity,
	})
	return nil
}

func (p *ProductAggregate) record(evt ProductEvent) {
	p.Changeset().Record(evt)
	_ = p.apply(evt)
}

func (p *ProductAggregate) apply(evt ProductEvent) error {
	switch e := evt.(type) {
	case ProductCreated:
		p.name = e.ProductName
		p.stock = e.Stock
	case ProductRenamed:
		p.name = e.ProductName
	case StockAdjusted:
		p.stock += e.Quantity
	default:
		return fmt.Errorf("unhandled product event: %s", evt.Name())
	}
	return nil
}
