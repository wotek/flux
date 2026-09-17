package flux

import "context"

// Saga defines the contract for a process manager.
// It leverages Go 1.26 self-referencing constraints for reflection-free instantiation.
type Saga[S Saga[S]] interface {
	// Identifier returns the globally unique ID of this saga instance.
	// This is typically derived from the CorrelationIdentifier of the triggering event.
	Identifier() Identifier

	// New creates a new, empty instance of the saga.
	// This is called on a nil pointer by the Orchestrator during loading.
	New() S
}

// SagaStore defines how the internal state of a saga is persisted between events.
type SagaStore[S Saga[S]] interface {
	// Load retrieves the saga state. The store is responsible for instantiating it.
	Load(ctx context.Context, id Identifier) (S, error)

	// Save persists the saga's state alongside any enqueued commands within the SAME
	// database transaction. A separate relay process is expected to poll these commands
	// and forward them to the CommandBus to achieve At-Least-Once (Outbox) delivery.
	Save(ctx context.Context, saga S, commands []any) error
}
