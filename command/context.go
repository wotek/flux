package command

import (
	"context"

	"github.com/wotek/flux"
)

// Context provides strongly-typed access to command metadata.
type Context interface {
	flux.Context
	CommandIdentifier() flux.Identifier
}

type commandContext struct {
	flux.Context
	cmdID flux.Identifier
}

func (c *commandContext) CommandIdentifier() flux.Identifier {
	return c.cmdID
}

// NewContext creates a new command Context with the given metadata.
func NewContext(parent context.Context, cmdID flux.Identifier, actor flux.Actor, correlationID flux.Identifier, causationID flux.Identifier) Context {
	return &commandContext{
		Context: flux.NewContext(parent, actor, correlationID, causationID),
		cmdID:   cmdID,
	}
}
