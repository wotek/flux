package queries

import (
	"github.com/wotek/flux"
	"github.com/wotek/flux/example/todo"
	"github.com/wotek/flux/example/todo/events"
	"github.com/wotek/flux/query"
)

// TodoList represents the active and archived tasks for a todo list.
type TodoList struct {
	ListIdentifier string   `json:"list_identifier"`
	Active         []string `json:"active"`
	Archived       []string `json:"archived"`
}

// GetTodoList is a read-only query requesting the tasks in a specific todo list.
type GetTodoList struct {
	ListIdentifier flux.Identifier
}

// GetTodoListHandler executes the [GetTodoList] query against the aggregate repository.
type GetTodoListHandler struct {
	repo *flux.AggregateRepository[*todo.TodoListAggregate, events.TodoEvent]
}

// NewGetTodoListHandler constructs a new [GetTodoListHandler].
func NewGetTodoListHandler(repo *flux.AggregateRepository[*todo.TodoListAggregate, events.TodoEvent]) *GetTodoListHandler {
	return &GetTodoListHandler{repo: repo}
}

// Handle executes the [GetTodoList] query, returning a [TodoList] snapshot.
func (h *GetTodoListHandler) Handle(ctx query.Context, q GetTodoList) (TodoList, error) {
	stream := flux.Stream{Identifier: q.ListIdentifier}
	list, err := h.repo.Load(ctx, stream)
	if err != nil {
		var zero *todo.TodoListAggregate
		list = zero.New(stream)
	}

	active := list.ActiveTasks()
	if active == nil {
		active = []string{}
	}
	archived := list.ArchivedTasks()
	if archived == nil {
		archived = []string{}
	}

	return TodoList{
		ListIdentifier: q.ListIdentifier.String(),
		Active:         active,
		Archived:       archived,
	}, nil
}

// RegisterGetTodoListHandler registers the [GetTodoListHandler] on the given query bus.
func RegisterGetTodoListHandler(bus *query.Bus, repo *flux.AggregateRepository[*todo.TodoListAggregate, events.TodoEvent]) {
	query.RegisterHandler(bus, NewGetTodoListHandler(repo))
}
