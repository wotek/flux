package queries

import (
	"fmt"

	"github.com/wotek/flux/example/todo/projections"
	"github.com/wotek/flux/query"
)

// GetCounter is a read-only query requesting the current snapshot of [projections.Counter] statistics.
type GetCounter struct{}

// GetCounterHandler executes the [GetCounter] query against a [projections.CounterStore].
type GetCounterHandler struct {
	statsStore projections.CounterStore
}

// NewGetCounterHandler constructs a new [GetCounterHandler].
func NewGetCounterHandler(statsStore projections.CounterStore) *GetCounterHandler {
	return &GetCounterHandler{statsStore: statsStore}
}

// Handle executes the [GetCounter] query, returning the current [projections.Counter].
func (h *GetCounterHandler) Handle(ctx query.Context, _ GetCounter) (projections.Counter, error) {
	counter, err := h.statsStore.GetCounter(ctx)
	if err != nil {
		return projections.Counter{}, fmt.Errorf("retrieving counter stats: %w", err)
	}
	return counter, nil
}

// RegisterGetCounterHandler registers the [GetCounterHandler] on the given query bus.
func RegisterGetCounterHandler(bus *query.Bus, statsStore projections.CounterStore) {
	query.RegisterHandler(bus, NewGetCounterHandler(statsStore))
}
