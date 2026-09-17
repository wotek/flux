package flux

import (
	"context"
)

// Context is the base typed context for the framework, providing guaranteed
// access to audit metadata (Actor, CorrelationIdentifier, CausationIdentifier)
// across all operations.
type Context interface {
	context.Context
	Actor() Actor
	CorrelationIdentifier() Identifier
	CausationIdentifier() Identifier
}

type baseContext struct {
	context.Context
	actor       Actor
	correlation Identifier
	causation   Identifier
}

func (b *baseContext) Actor() Actor                      { return b.actor }
func (b *baseContext) CorrelationIdentifier() Identifier { return b.correlation }
func (b *baseContext) CausationIdentifier() Identifier   { return b.causation }

// NewContext creates a new generic base Context.
func NewContext(parent context.Context, actor Actor, correlationId Identifier, causationId Identifier) Context {
	return &baseContext{
		Context:     parent,
		actor:       actor,
		correlation: correlationId,
		causation:   causationId,
	}
}
