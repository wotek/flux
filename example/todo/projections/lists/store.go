package lists

import (
	"context"

	"github.com/wotek/flux"
	"github.com/wotek/flux/projection"
)

// Store defines the persistence contract for maintaining the lists read model.
type Store interface {
	// UpsertList creates or updates a list's title in the read model.
	UpsertList(ctx projection.Context, id flux.Identifier, title string) error

	// UpdateCounts updates the active and archived task counts for a list.
	UpdateCounts(ctx projection.Context, id flux.Identifier, deltaActive, deltaArchived int) error

	// GetLists retrieves a snapshot of all known todo lists ordered by title/identifier.
	GetLists(ctx context.Context) ([]ListSummary, error)
}
