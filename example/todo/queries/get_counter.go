package queries

import (
	"fmt"

	"github.com/wotek/flux/example/todo/projections/counter"
	"github.com/wotek/flux/query"
)

// GetCounter is a read-only query requesting the current snapshot of [counter.Counter] statistics.
type GetCounter struct{}

// GetCounterHandler executes the [GetCounter] query against a [counter.Store].
type GetCounterHandler struct {
	statsStore counter.Store
}

// NewGetCounterHandler constructs a new [GetCounterHandler].
func NewGetCounterHandler(statsStore counter.Store) *GetCounterHandler {
	return &GetCounterHandler{statsStore: statsStore}
}

// Handle executes the [GetCounter] query, returning the current [counter.Counter].
func (h *GetCounterHandler) Handle(ctx query.Context, _ GetCounter) (counter.Counter, error) {
	stats, err := h.statsStore.GetCounter(ctx)
	if err != nil {
		return counter.Counter{}, fmt.Errorf("retrieving counter stats: %w", err)
	}
	return stats, nil
}

// RegisterGetCounterHandler registers the [GetCounterHandler] on the given query bus.
func RegisterGetCounterHandler(bus *query.Bus, statsStore counter.Store) {
	query.RegisterHandler(bus, NewGetCounterHandler(statsStore))
}
