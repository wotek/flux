package todo

import (
	"context"
)

// CounterStore defines the persistence contract for tracking and querying task counter metrics.
type CounterStore interface {
	// IncrementActive modifies the count of active tasks by the given delta.
	IncrementActive(ctx context.Context, delta int) error

	// IncrementArchived modifies the count of archived tasks by the given delta.
	IncrementArchived(ctx context.Context, delta int) error

	// IncrementRemoved modifies the count of removed tasks by the given delta.
	IncrementRemoved(ctx context.Context, delta int) error

	// GetCounter retrieves the current snapshot of task metrics.
	GetCounter(ctx context.Context) (Counter, error)
}
