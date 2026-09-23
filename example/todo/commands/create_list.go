package commands

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/todo"
	"github.com/wotek/flux/example/todo/events"
)

// CreateList is a command instructing the system to initialize a new todo list aggregate.
type CreateList struct {
	ListIdentifier flux.Identifier
	Title          string
}

// CreateListHandler executes the [CreateList] command against the aggregate repository.
type CreateListHandler struct {
	repo *flux.AggregateRepository[*todo.TodoListAggregate, events.TodoEvent]
}

// NewCreateListHandler constructs a new [CreateListHandler].
func NewCreateListHandler(repo *flux.AggregateRepository[*todo.TodoListAggregate, events.TodoEvent]) *CreateListHandler {
	return &CreateListHandler{repo: repo}
}

// Handle processes the [CreateList] command, creating the aggregate if not yet initialized.
func (h *CreateListHandler) Handle(ctx command.Context, cmd CreateList) error {
	stream := flux.Stream{Identifier: cmd.ListIdentifier}
	list, err := h.repo.Load(ctx, stream)
	if err != nil {
		var zero *todo.TodoListAggregate
		list = zero.New(stream)
	}

	list.Create(cmd.Title)
	if err := h.repo.Save(ctx, list); err != nil {
		return fmt.Errorf("saving todo list after creation: %w", err)
	}
	return nil
}

// RegisterCreateListHandler registers the [CreateListHandler] on the given command bus.
func RegisterCreateListHandler(bus *command.Bus, repo *flux.AggregateRepository[*todo.TodoListAggregate, events.TodoEvent]) {
	command.RegisterHandler(bus, NewCreateListHandler(repo))
}
