package saga

import (
	"github.com/wotek/flux/event"
)

// Context extends event.Context, giving saga handlers the ability to dispatch commands.
type Context interface {
	event.Context

	// dispatch is unexported. It is used internally by EnqueueCommand.
	dispatch(cmd any)

	// QueuedCommands returns all commands enqueued during the event handling.
	QueuedCommands() []any
}

type sagaContext struct {
	event.Context
	queuedCommands []any
}

func (s *sagaContext) dispatch(cmd any) {
	s.queuedCommands = append(s.queuedCommands, cmd)
}

func (s *sagaContext) QueuedCommands() []any {
	return s.queuedCommands
}

// NewContext creates a new Saga Context from an event.Context.
func NewContext(parent event.Context) Context {
	return &sagaContext{
		Context:        parent,
		queuedCommands: make([]any, 0),
	}
}

// NewSagaContext is an alias for NewContext.
func NewSagaContext(parent event.Context) Context {
	return NewContext(parent)
}

// EnqueueCommand safely queues a strongly-typed command to be dispatched by the saga outbox.
func EnqueueCommand[C any](ctx Context, cmd C) {
	ctx.dispatch(cmd)
}
