package workflow

import (
	"context"
	"slices"

	"github.com/wotek/flux"
	"github.com/wotek/flux/event"
)

// Context extends event metadata, giving workflow handlers the ability to dispatch commands.
type Context interface {
	event.EventMetadata

	// dispatch is unexported. It is used internally by EnqueueCommand.
	dispatch(cmd any)

	// QueuedCommands returns all commands enqueued during the event handling.
	QueuedCommands() []any

	// WithParent returns a new Context with the underlying Go context replaced.
	WithParent(parent context.Context) Context
}

type workflowContext struct {
	event.Context
	queuedCommands *[]any
}

var _ Context = (*workflowContext)(nil)
var _ event.EventMetadata = (*workflowContext)(nil)
var _ flux.Context = (*workflowContext)(nil)

func (w *workflowContext) dispatch(cmd any) {
	*w.queuedCommands = append(*w.queuedCommands, cmd)
}

func (w *workflowContext) QueuedCommands() []any {
	return slices.Clone(*w.queuedCommands)
}

// WithParent returns a clone of the workflow Context with the underlying Go context replaced,
// sharing the mutable command queue pointer across child contexts.
func (w *workflowContext) WithParent(parent context.Context) Context {
	return &workflowContext{
		Context:        w.Context.WithParent(parent),
		queuedCommands: w.queuedCommands,
	}
}

// NewContext creates a new Workflow Context from an event.Context.
func NewContext(parent event.Context) Context {
	cmds := make([]any, 0)
	return &workflowContext{
		Context:        parent,
		queuedCommands: &cmds,
	}
}

// EnqueueCommand safely queues a strongly-typed command to be dispatched by the workflow outbox.
func EnqueueCommand[C any](ctx Context, cmd C) {
	ctx.dispatch(cmd)
}

