package queries

import (
	"github.com/wotek/flux/example/todo/projections"
	"github.com/wotek/flux/query"
)

// RegisterHandlers registers all query handlers in this package on the given query bus.
func RegisterHandlers(bus *query.Bus, statsStore projections.CounterStore) {
	RegisterGetCounterHandler(bus, statsStore)
}
