package saga

import (
	"github.com/wotek/flux"
)

// Context extends flux.EventContext, giving saga handlers the ability to dispatch commands.
type Context interface {
	flux.EventContext

	// dispatch is unexported. It is used internally by EnqueueCommand.
	dispatch(cmd any)

	// QueuedCommands returns all commands enqueued during the event handling.
	QueuedCommands() []any
}

type sagaContext struct {
	flux.EventContext
	queuedCommands []any
}

func (s *sagaContext) dispatch(cmd any) {
	s.queuedCommands = append(s.queuedCommands, cmd)
}

func (s *sagaContext) QueuedCommands() []any {
	return s.queuedCommands
}

// NewContext creates a new Saga Context from an EventContext.
func NewContext(parent flux.EventContext) Context {
	return &sagaContext{
		EventContext:   parent,
		queuedCommands: make([]any, 0),
	}
}

// NewSagaContext is an alias for NewContext.
func NewSagaContext(parent flux.EventContext) Context {
	return NewContext(parent)
}

// EnqueueCommand safely queues a strongly-typed command to be dispatched by the saga outbox.
func EnqueueCommand[C any](ctx Context, cmd C) {
	ctx.dispatch(cmd)
}
