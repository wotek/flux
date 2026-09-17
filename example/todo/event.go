package todo

import "github.com/wotek/flux"

// TodoEvent defines the sealed interface for all domain events emitted by a [TodoListAggregate].
type TodoEvent interface {
	flux.Event
	isTodoEvent()
}
