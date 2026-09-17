package commands

import (
	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/todo"
	"github.com/wotek/flux/example/todo/events"
)

// RegisterHandlers registers all command handlers in this package on the given command bus.
func RegisterHandlers(bus *command.Bus, repo *flux.AggregateRepository[*todo.TodoListAggregate, events.TodoEvent]) {
	RegisterAddTaskHandler(bus, repo)
	RegisterRemoveTaskHandler(bus, repo)
	RegisterDoneTasksHandler(bus, repo)
}
