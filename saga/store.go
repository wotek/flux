package saga

import (
	"context"

	"github.com/wotek/flux"
)

// Store defines how the internal state of a saga is persisted between events.
type Store[S Saga[S]] interface {
	// Load retrieves the saga state. The store is responsible for instantiating it.
	Load(ctx context.Context, id flux.Identifier) (S, error)

	// Save persists the saga's state alongside any enqueued commands within the SAME
	// database transaction. A separate relay process is expected to poll these commands
	// and forward them to the CommandBus to achieve At-Least-Once (Outbox) delivery.
	Save(ctx context.Context, saga S, commands []any) error
}

// SagaStore is an alias for Store to provide explicit naming when desired.
type SagaStore[S Saga[S]] = Store[S]
