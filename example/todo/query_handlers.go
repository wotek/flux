package todo

import (
	"fmt"

	"github.com/wotek/flux/query"
)

// RegisterQueryHandlers registers query handlers for read models on the given [query.Bus].
func RegisterQueryHandlers(bus *query.Bus, statsStore CounterStore) {
	query.Register(bus, func(ctx query.Context, _ GetCounter) (Counter, error) {
		counter, err := statsStore.GetCounter(ctx)
		if err != nil {
			return Counter{}, fmt.Errorf("retrieving counter stats: %w", err)
		}
		return counter, nil
	})
}
