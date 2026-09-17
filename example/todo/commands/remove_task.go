package commands

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/todo"
	"github.com/wotek/flux/example/todo/events"
)

// RemoveTask is a command instructing the system to remove a task from a specific todo list.
type RemoveTask struct {
	ListIdentifier flux.Identifier
	Task           string
}

// RemoveTaskHandler executes the [RemoveTask] command against the aggregate repository.
type RemoveTaskHandler struct {
	repo *flux.AggregateRepository[*todo.TodoListAggregate, events.TodoEvent]
}

// NewRemoveTaskHandler constructs a new [RemoveTaskHandler].
func NewRemoveTaskHandler(repo *flux.AggregateRepository[*todo.TodoListAggregate, events.TodoEvent]) *RemoveTaskHandler {
	return &RemoveTaskHandler{repo: repo}
}

// Handle processes the [RemoveTask] command, removing the task from the aggregate.
func (h *RemoveTaskHandler) Handle(ctx command.Context, cmd RemoveTask) error {
	stream := flux.Stream{Identifier: cmd.ListIdentifier}
	list, err := h.repo.Load(ctx, stream)
	if err != nil {
		return fmt.Errorf("loading todo list for task removal: %w", err)
	}

	list.Remove(cmd.Task)
	if err := h.repo.Save(ctx, list); err != nil {
		return fmt.Errorf("saving todo list after removing task: %w", err)
	}
	return nil
}

// RegisterRemoveTaskHandler registers the [RemoveTaskHandler] on the given command bus.
func RegisterRemoveTaskHandler(bus *command.Bus, repo *flux.AggregateRepository[*todo.TodoListAggregate, events.TodoEvent]) {
	command.RegisterHandler(bus, NewRemoveTaskHandler(repo))
}
