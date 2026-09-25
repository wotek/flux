package workflow

import (
	"context"

	"github.com/wotek/flux"
)

// Store defines how the internal state of a workflow is persisted between events.
type Store[W Workflow[W]] interface {
	// Load retrieves the workflow state. The store is responsible for instantiating it.
	Load(ctx context.Context, id flux.Identifier) (W, error)

	// Save persists the workflow's state alongside any enqueued commands within the SAME
	// database transaction. A separate relay process polls these commands and forwards
	// them to the CommandBus to achieve At-Least-Once (Outbox) delivery.
	// Because at-least-once delivery may re-dispatch commands upon restart or retry,
	// target command handlers must be idempotent.
	Save(ctx context.Context, workflow W, commands []any) error
}

// WorkflowStore is an alias for Store to provide explicit naming when desired.
type WorkflowStore[W Workflow[W]] = Store[W]
