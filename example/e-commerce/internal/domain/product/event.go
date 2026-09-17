package product

import (
	"github.com/wotek/flux"
)

// ProductEvent is a marker interface for all product domain events.
type ProductEvent interface {
	flux.Event
	isProductEvent()
}
