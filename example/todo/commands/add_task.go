package commands

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/todo"
	"github.com/wotek/flux/example/todo/events"
)

// AddTask is a command instructing the system to add a new task to a specific todo list.
type AddTask struct {
	ListIdentifier flux.Identifier
	Task           string
}

// AddTaskHandler executes the [AddTask] command against the aggregate repository.
type AddTaskHandler struct {
	repo *flux.AggregateRepository[*todo.TodoListAggregate, events.TodoEvent]
}

// NewAddTaskHandler constructs a new [AddTaskHandler].
func NewAddTaskHandler(repo *flux.AggregateRepository[*todo.TodoListAggregate, events.TodoEvent]) *AddTaskHandler {
	return &AddTaskHandler{repo: repo}
}

// Handle processes the [AddTask] command, creating or updating the aggregate.
func (h *AddTaskHandler) Handle(ctx command.Context, cmd AddTask) error {
	stream := flux.Stream{Identifier: cmd.ListIdentifier}
	list, err := h.repo.Load(ctx, stream)
	if err != nil {
		var zero *todo.TodoListAggregate
		list = zero.New(stream)
	}

	list.Add(cmd.Task)
	if err := h.repo.Save(ctx, list); err != nil {
		return fmt.Errorf("saving todo list after adding task: %w", err)
	}
	return nil
}

// RegisterAddTaskHandler registers the [AddTaskHandler] on the given command bus.
func RegisterAddTaskHandler(bus *command.Bus, repo *flux.AggregateRepository[*todo.TodoListAggregate, events.TodoEvent]) {
	command.RegisterHandler(bus, NewAddTaskHandler(repo))
}
