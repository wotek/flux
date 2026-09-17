package events

import "github.com/wotek/flux"

// TodoEvent defines the sealed interface for all domain events emitted by a todo list aggregate.
type TodoEvent interface {
	flux.Event
	isTodoEvent()
}
