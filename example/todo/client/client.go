package client

import (
	"context"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/todo/projections/counter"
	"github.com/wotek/flux/example/todo/queries"
)

// Client abstracts interaction with the Todo CQRS system,
// whether dispatched locally via in-memory buses or remotely over HTTP.
type Client interface {
	// AddTask dispatches a command to append a task to the specified list.
	AddTask(ctx context.Context, listIdentifier flux.Identifier, task string) error

	// RemoveTask dispatches a command to remove an existing task from the specified list.
	RemoveTask(ctx context.Context, listIdentifier flux.Identifier, task string) error

	// DoneTasks dispatches a command to mark one or more tasks as completed.
	DoneTasks(ctx context.Context, listIdentifier flux.Identifier, tasks ...string) error

	// GetCounter queries the counter read-model projection.
	GetCounter(ctx context.Context) (counter.Counter, error)

	// GetTodoList queries the active and archived tasks for a given list.
	GetTodoList(ctx context.Context, listIdentifier flux.Identifier) (queries.TodoList, error)
}
