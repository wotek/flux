// Package todo provides a reference implementation of a Todo list application
// built on the flux event-sourcing and CQRS framework.
//
// The package demonstrates:
//   - Pure domain modeling with [flux.AggregateRoot] and [TodoListAggregate].
//   - Strongly-typed, reflection-free domain events ([TaskAdded], [TaskRemoved], [TasksDone]).
//   - Decoupled commands ([AddTask], [RemoveTask], [DoneTasks]) routed via [command.Bus].
//   - Asynchronous read-model projections ([Counter]) backed by [projection.Projector].
//   - Thread-safe storage abstractions using [CounterStore].
//   - Queries ([GetCounter]) dispatched through [query.Bus].
package todo
