package projection

import (
	"context"

	"github.com/wotek/flux"
	"github.com/wotek/flux/event"
)

// Context provides a distinct type boundary guaranteeing that the context
// is bound to the projection's active database transaction.
type Context interface {
	event.EventMetadata
	WithParent(parent context.Context) Context
}

type projectionContext struct {
	event.Context
}

var _ Context = (*projectionContext)(nil)
var _ event.EventMetadata = (*projectionContext)(nil)
var _ flux.Context = (*projectionContext)(nil)

// WithParent returns a clone of the projection Context with the underlying Go context replaced.
func (p *projectionContext) WithParent(parent context.Context) Context {
	return &projectionContext{
		Context: p.Context.WithParent(parent),
	}
}

// NewContext creates a new projection Context from an event.Context.
func NewContext(parent event.Context) Context {
	return &projectionContext{
		Context: parent,
	}
}

