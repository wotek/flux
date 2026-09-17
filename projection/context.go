package projection

import (
	"github.com/wotek/flux"
)

// Context extends flux.EventContext. It acts as a distinct type boundary
// guaranteeing that the context is bound to the projection's active database transaction.
type Context interface {
	flux.EventContext
}

type projectionContext struct {
	flux.EventContext
}

// NewContext creates a new projection Context from an EventContext.
func NewContext(parent flux.EventContext) Context {
	return &projectionContext{
		EventContext: parent,
	}
}

// NewProjectionContext is an alias for NewContext.
func NewProjectionContext(parent flux.EventContext) Context {
	return NewContext(parent)
}
