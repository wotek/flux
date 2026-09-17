package todo

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/example/todo/commands"
	"github.com/wotek/flux/example/todo/events"
)

// RegisterCommandHandlers registers command handlers for todo domain operations on the given [command.Bus].
func RegisterCommandHandlers(bus *command.Bus, repo *flux.AggregateRepository[*TodoListAggregate, events.TodoEvent]) {
	command.Register(bus, func(ctx command.Context, cmd commands.AddTask) error {
		stream := flux.Stream{Identifier: cmd.ListIdentifier}
		list, err := repo.Load(ctx, stream)
		if err != nil {
			var zero *TodoListAggregate
			list = zero.New(stream)
		}

		list.Add(cmd.Task)
		if err := repo.Save(ctx, list); err != nil {
			return fmt.Errorf("saving todo list after adding task: %w", err)
		}
		return nil
	})

	command.Register(bus, func(ctx command.Context, cmd commands.RemoveTask) error {
		stream := flux.Stream{Identifier: cmd.ListIdentifier}
		list, err := repo.Load(ctx, stream)
		if err != nil {
			return fmt.Errorf("loading todo list for task removal: %w", err)
		}

		list.Remove(cmd.Task)
		if err := repo.Save(ctx, list); err != nil {
			return fmt.Errorf("saving todo list after removing task: %w", err)
		}
		return nil
	})

	command.Register(bus, func(ctx command.Context, cmd commands.DoneTasks) error {
		stream := flux.Stream{Identifier: cmd.ListIdentifier}
		list, err := repo.Load(ctx, stream)
		if err != nil {
			return fmt.Errorf("loading todo list for marking tasks done: %w", err)
		}

		list.Done(cmd.Tasks...)
		if err := repo.Save(ctx, list); err != nil {
			return fmt.Errorf("saving todo list after marking tasks done: %w", err)
		}
		return nil
	})
}

// RegisterHandlers is an alias for [RegisterCommandHandlers] to maintain naming compatibility with the design guide.
func RegisterHandlers(bus *command.Bus, repo *flux.AggregateRepository[*TodoListAggregate, events.TodoEvent]) {
	RegisterCommandHandlers(bus, repo)
}
