package workflow

import (
	"github.com/wotek/flux/event"
)

// Context extends event.Context, giving workflow handlers the ability to dispatch commands.
type Context interface {
	event.Context

	// dispatch is unexported. It is used internally by EnqueueCommand.
	dispatch(cmd any)

	// QueuedCommands returns all commands enqueued during the event handling.
	QueuedCommands() []any
}

type workflowContext struct {
	event.Context
	queuedCommands []any
}

func (w *workflowContext) dispatch(cmd any) {
	w.queuedCommands = append(w.queuedCommands, cmd)
}

func (w *workflowContext) QueuedCommands() []any {
	return w.queuedCommands
}

// NewContext creates a new Workflow Context from an event.Context.
func NewContext(parent event.Context) Context {
	return &workflowContext{
		Context:        parent,
		queuedCommands: make([]any, 0),
	}
}

// EnqueueCommand safely queues a strongly-typed command to be dispatched by the workflow outbox.
func EnqueueCommand[C any](ctx Context, cmd C) {
	ctx.dispatch(cmd)
}
