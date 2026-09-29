package checkpoint

import (
	"context"

	"github.com/wotek/flux"
)

// Store persists the last successfully processed global event-stream position
// for a named consumer (projector, orchestrator, or other tailing worker).
//
// Positions are monotonic per consumer identifier. SetPosition must not move
// the stored position backward: if the proposed position is less than the
// currently stored position, the call succeeds without changing the stored
// value (compare-and-set maximum / ignore-older). This survives at-least-once
// retries that re-issue an earlier SetPosition after a partial failure.
//
// GetPosition returns 0 when no row exists for id (consumer has never
// checkpointed). Missing consumer is not an error.
type Store interface {
	// GetPosition returns the last successfully processed global stream position
	// for the consumer identified by id. Returns 0 if the consumer has no
	// stored checkpoint.
	GetPosition(ctx context.Context, id flux.Identifier) (uint64, error)

	// SetPosition records that the consumer identified by id has successfully
	// processed the global stream through position. Implementations must apply
	// monotonic max semantics: older positions must not overwrite newer ones.
	SetPosition(ctx context.Context, id flux.Identifier, position uint64) error
}
