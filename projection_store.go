package flux

import "context"

// ProjectionStore defines the contract for persisting both the read-model data and its cursor.
// It abstracts the transactional boundaries of the underlying database.
type ProjectionStore interface {
	// GetPosition retrieves the last successfully processed global position for the projection.
	GetPosition(ctx context.Context, id Identifier) (uint64, error)

	// Update runs a database transaction. It provides the framework with a transactional
	// context and executes the `mutate` closure (which contains the user's read-model logic).
	// If the closure succeeds, the framework commits the transaction, atomically saving the
	// read-model changes AND the new envelope.Position.
	Update(ctx context.Context, id Identifier, env Envelope, mutate func(txCtx context.Context) error) error
}
