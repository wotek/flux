package commands

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/todo"
	"github.com/wotek/flux/example/todo/events"
)

// DoneTasks is a command instructing the system to mark one or more tasks as completed.
type DoneTasks struct {
	ListIdentifier flux.Identifier
	Tasks          []string
}

// DoneTasksHandler executes the [DoneTasks] command against the aggregate repository.
type DoneTasksHandler struct {
	repo *flux.AggregateRepository[*todo.TodoListAggregate, events.TodoEvent]
}

// NewDoneTasksHandler constructs a new [DoneTasksHandler].
func NewDoneTasksHandler(repo *flux.AggregateRepository[*todo.TodoListAggregate, events.TodoEvent]) *DoneTasksHandler {
	return &DoneTasksHandler{repo: repo}
}

// Handle processes the [DoneTasks] command, marking tasks as completed in the aggregate.
func (h *DoneTasksHandler) Handle(ctx command.Context, cmd DoneTasks) error {
	stream := flux.Stream{Identifier: cmd.ListIdentifier}
	list, err := h.repo.Load(ctx, stream)
	if err != nil {
		return fmt.Errorf("loading todo list for marking tasks done: %w", err)
	}

	list.Done(cmd.Tasks...)
	if err := h.repo.Save(ctx, list); err != nil {
		return fmt.Errorf("saving todo list after marking tasks done: %w", err)
	}
	return nil
}

// RegisterDoneTasksHandler registers the [DoneTasksHandler] on the given command bus.
func RegisterDoneTasksHandler(bus *command.Bus, repo *flux.AggregateRepository[*todo.TodoListAggregate, events.TodoEvent]) {
	command.RegisterHandler(bus, NewDoneTasksHandler(repo))
}
