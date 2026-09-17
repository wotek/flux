package projection

import (
	"context"

	"github.com/wotek/flux"
)

// Store defines the persistence contract for maintaining read-model state
// and tracking event stream checkpoints transactionally.
type Store interface {
	GetPosition(ctx context.Context, id flux.Identifier) (uint64, error)
	Update(ctx context.Context, id flux.Identifier, env flux.Envelope, mutate func(txCtx context.Context) error) error
}

// ProjectionStore is an alias for Store to provide explicit naming when desired.
type ProjectionStore = Store
