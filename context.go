package flux

import (
	"context"
)

// Context is the base typed context for the framework, providing guaranteed
// access to metadata for any operation.
type Context interface {
	context.Context
	Actor() Actor
	CorrelationIdentifier() Identifier
	CausationIdentifier() Identifier
}

// CommandContext extends the base Context for command execution.
type CommandContext interface {
	Context
	CommandIdentifier() Identifier
}

// QueryContext extends the base Context for read-only query operations.
type QueryContext interface {
	Context
	QueryIdentifier() Identifier
}

// EventContext provides strongly-typed access to the Envelope metadata
// while keeping the event payload clean.
type EventContext interface {
	Context
	EventIdentifier() Identifier
	Stream() Stream
	Revision() uint64
	Position() uint64
	Metadata() map[string]string
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

type commandContext struct {
	*baseContext
	cmdID Identifier
}

func (c *commandContext) CommandIdentifier() Identifier { return c.cmdID }

// NewCommandContext creates a new CommandContext with the given metadata.
func NewCommandContext(parent context.Context, cmdId Identifier, actor Actor, correlationId Identifier, causationId Identifier) CommandContext {
	return &commandContext{
		baseContext: &baseContext{
			Context:     parent,
			actor:       actor,
			correlation: correlationId,
			causation:   causationId,
		},
		cmdID: cmdId,
	}
}

type queryContext struct {
	*baseContext
	queryID Identifier
}

func (q *queryContext) QueryIdentifier() Identifier { return q.queryID }

// NewQueryContext creates a new QueryContext with the given metadata.
func NewQueryContext(parent context.Context, queryId Identifier, actor Actor, correlationId Identifier, causationId Identifier) QueryContext {
	return &queryContext{
		baseContext: &baseContext{
			Context:     parent,
			actor:       actor,
			correlation: correlationId,
			causation:   causationId,
		},
		queryID: queryId,
	}
}

type eventContext struct {
	*baseContext
	eventID  Identifier
	stream   Stream
	revision uint64
	position uint64
	metadata map[string]string
}

func (e *eventContext) EventIdentifier() Identifier { return e.eventID }
func (e *eventContext) Stream() Stream              { return e.stream }
func (e *eventContext) Revision() uint64            { return e.revision }
func (e *eventContext) Position() uint64            { return e.position }
func (e *eventContext) Metadata() map[string]string { return e.metadata }

// NewEventContext creates a new EventContext from an Envelope.
func NewEventContext(parent context.Context, env Envelope) EventContext {
	return &eventContext{
		baseContext: &baseContext{
			Context:     parent,
			actor:       env.Actor,
			correlation: env.CorrelationIdentifier,
			causation:   env.Identifier,
		},
		eventID:  env.Identifier,
		stream:   env.Stream,
		revision: env.Revision,
		position: env.Position,
		metadata: env.Metadata,
	}
}

// NewContext creates a new generic base Context.
func NewContext(parent context.Context, actor Actor, correlationId Identifier, causationId Identifier) Context {
	return &baseContext{
		Context:     parent,
		actor:       actor,
		correlation: correlationId,
		causation:   causationId,
	}
}
