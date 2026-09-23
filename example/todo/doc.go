// Package todo provides a reference implementation of a Todo list application
// built on the flux event-sourcing and CQRS framework.
//
// The package is organized into domain-specific subpackages following vertical slices:
//   - [github.com/wotek/flux/example/todo/events]: Domain events emitted by the aggregate.
//   - [github.com/wotek/flux/example/todo/commands]: Strongly-typed commands and their co-located handlers.
//   - [github.com/wotek/flux/example/todo/queries]: Strongly-typed queries and their co-located handlers.
//   - [github.com/wotek/flux/example/todo/projections]: Read models, storage contracts, and projectors.
//
// The root package contains the domain [TodoListAggregate].
package todo
