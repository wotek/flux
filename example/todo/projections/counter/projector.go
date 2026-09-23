package counter

import (
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/todo/events"
	"github.com/wotek/flux/projection"
)

// NewProjector creates and configures a [projection.Projector] that updates [Store]
// based on domain events emitted by todo lists.
func NewProjector(
	id flux.Identifier,
	eventStore flux.EventStore,
	projStore projection.Store,
	statsStore Store,
) *projection.Projector {
	projector := projection.New(id, eventStore, projStore)

	projection.RegisterHandler(projector, func(ctx projection.Context, _ events.TaskAdded) error {
		if err := statsStore.IncrementActive(ctx, 1); err != nil {
			return fmt.Errorf("updating active count for task added: %w", err)
		}
		return nil
	})

	projection.RegisterHandler(projector, func(ctx projection.Context, _ events.TaskRemoved) error {
		if err := statsStore.IncrementRemoved(ctx, 1); err != nil {
			return fmt.Errorf("updating removed count for task removed: %w", err)
		}
		if err := statsStore.IncrementActive(ctx, -1); err != nil {
			return fmt.Errorf("decrementing active count for task removed: %w", err)
		}
		return nil
	})

	projection.RegisterHandler(projector, func(ctx projection.Context, e events.TasksDone) error {
		count := len(e.Tasks)
		if err := statsStore.IncrementArchived(ctx, count); err != nil {
			return fmt.Errorf("updating archived count for tasks done: %w", err)
		}
		if err := statsStore.IncrementActive(ctx, -count); err != nil {
			return fmt.Errorf("decrementing active count for tasks done: %w", err)
		}
		return nil
	})

	return projector
}

// NewCounterProjector is an alias for [NewProjector].
func NewCounterProjector(
	id flux.Identifier,
	eventStore flux.EventStore,
	projStore projection.Store,
	statsStore Store,
) *projection.Projector {
	return NewProjector(id, eventStore, projStore, statsStore)
}

// StartCounterProjector is an alias for [NewProjector] maintaining consistency with the design guide.
func StartCounterProjector(
	id flux.Identifier,
	eventStore flux.EventStore,
	projStore projection.Store,
	statsStore Store,
) *projection.Projector {
	return NewProjector(id, eventStore, projStore, statsStore)
}
