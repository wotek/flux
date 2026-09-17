package todo

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
)

// RegisterCommandHandlers registers command handlers for todo domain operations on the given [command.Bus].
func RegisterCommandHandlers(bus *command.Bus, repo *flux.AggregateRepository[*TodoListAggregate, TodoEvent]) {
	command.Register(bus, func(ctx command.Context, cmd AddTask) error {
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

	command.Register(bus, func(ctx command.Context, cmd RemoveTask) error {
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

	command.Register(bus, func(ctx command.Context, cmd DoneTasks) error {
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
func RegisterHandlers(bus *command.Bus, repo *flux.AggregateRepository[*TodoListAggregate, TodoEvent]) {
	RegisterCommandHandlers(bus, repo)
}
