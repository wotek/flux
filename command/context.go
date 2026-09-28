package command

import (
	"context"

	"github.com/wotek/flux"
)

// Context provides strongly-typed access to command metadata.
type Context interface {
	flux.Context
	CommandIdentifier() flux.Identifier
	WithParent(parent context.Context) Context
}

type commandContext struct {
	flux.Context
	cmdID flux.Identifier
}

var _ Context = (*commandContext)(nil)
var _ flux.Context = (*commandContext)(nil)

func (c *commandContext) CommandIdentifier() flux.Identifier {
	return c.cmdID
}

// WithParent returns a clone of the command Context with the underlying Go context replaced.
func (c *commandContext) WithParent(parent context.Context) Context {
	return &commandContext{
		Context: flux.NewContext(
			parent,
			c.Actor(),
			c.CorrelationIdentifier(),
			c.CausationIdentifier(),
			flux.WithInstrumentation(c.Instrumentation()),
		),
		cmdID: c.cmdID,
	}
}

// NewContext creates a new command Context with the given metadata.
func NewContext(parent context.Context, cmdID flux.Identifier, actor flux.Actor, correlationID flux.Identifier, causationID flux.Identifier, opts ...flux.ContextOption) Context {
	return &commandContext{
		Context: flux.NewContext(parent, actor, correlationID, causationID, opts...),
		cmdID:   cmdID,
	}
}

