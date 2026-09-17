// Package todo provides a reference implementation of a Todo list application
// built on the flux event-sourcing and CQRS framework.
//
// The package is organized into domain-specific subpackages:
//   - [github.com/wotek/flux/example/todo/events]: Domain events emitted by the aggregate.
//   - [github.com/wotek/flux/example/todo/commands]: Strongly-typed commands dispatched by clients.
//   - [github.com/wotek/flux/example/todo/projections]: Read models, storage contracts, and projectors.
//
// The root package contains the domain [TodoListAggregate] and command handlers wiring.
package todo
