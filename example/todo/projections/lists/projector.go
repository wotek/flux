package lists

import (
	"github.com/wotek/flux"
	"github.com/wotek/flux/example/todo/events"
	"github.com/wotek/flux/projection"
)

// NewProjector creates and configures a [projection.Projector] that updates the lists read model.
func NewProjector(
	id flux.Identifier,
	eventStore flux.EventStore,
	projStore projection.Store,
	listsStore Store,
) *projection.Projector {
	projector := projection.New(id, eventStore, projStore)

	projection.RegisterHandler(projector, func(ctx projection.Context, e events.ListCreated) error {
		return listsStore.UpsertList(ctx, ctx.Stream().Identifier, e.Title)
	})

	projection.RegisterHandler(projector, func(ctx projection.Context, _ events.TaskAdded) error {
		return listsStore.UpdateCounts(ctx, ctx.Stream().Identifier, 1, 0)
	})

	projection.RegisterHandler(projector, func(ctx projection.Context, _ events.TaskRemoved) error {
		return listsStore.UpdateCounts(ctx, ctx.Stream().Identifier, -1, 0)
	})

	projection.RegisterHandler(projector, func(ctx projection.Context, e events.TasksDone) error {
		count := len(e.Tasks)
		return listsStore.UpdateCounts(ctx, ctx.Stream().Identifier, -count, count)
	})

	return projector
}
