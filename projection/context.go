package projection

import (
	"github.com/wotek/flux/event"
)

// Context extends event.Context. It acts as a distinct type boundary
// guaranteeing that the context is bound to the projection's active database transaction.
type Context interface {
	event.Context
}

type projectionContext struct {
	event.Context
}

// NewContext creates a new projection Context from an event.Context.
func NewContext(parent event.Context) Context {
	return &projectionContext{
		Context: parent,
	}
}
