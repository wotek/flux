package queries

import (
	"github.com/wotek/flux/example/todo/projections/counter"
	"github.com/wotek/flux/query"
)

// RegisterHandlers registers all query handlers in this package on the given query bus.
func RegisterHandlers(bus *query.Bus, statsStore counter.Store) {
	RegisterGetCounterHandler(bus, statsStore)
}
