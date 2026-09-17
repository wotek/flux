package queries

import (
	"github.com/wotek/flux"
	"github.com/wotek/flux/example/todo"
	"github.com/wotek/flux/example/todo/events"
	"github.com/wotek/flux/example/todo/projections/counter"
	"github.com/wotek/flux/example/todo/projections/lists"
	"github.com/wotek/flux/query"
)

// RegisterHandlers registers all query handlers in this package on the given query bus.
func RegisterHandlers(
	bus *query.Bus,
	statsStore counter.Store,
	listsStore lists.Store,
	repo *flux.AggregateRepository[*todo.TodoListAggregate, events.TodoEvent],
) {
	RegisterGetCounterHandler(bus, statsStore)
	if listsStore != nil {
		RegisterGetListsHandler(bus, listsStore)
	}
	RegisterGetTodoListHandler(bus, repo)
}
