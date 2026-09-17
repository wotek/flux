package flux

import (
	"fmt"
	"reflect"
	"sync"
)

// QueryHandler represents a component that handles a specific query and returns a read model.
type QueryHandler[Q any, R any] interface {
	Handle(ctx QueryContext, query Q) (R, error)
}

// QueryBus manages the registration and routing of queries.
type QueryBus struct {
	mu       sync.RWMutex
	handlers map[reflect.Type]any
}

// NewQueryBus creates a new QueryBus instance.
func NewQueryBus() *QueryBus {
	return &QueryBus{
		handlers: make(map[reflect.Type]any),
	}
}

// RegisterQueryHandler registers a strongly-typed handler for a specific query type.
func RegisterQueryHandler[Q any, R any](bus *QueryBus, handler QueryHandler[Q, R]) {
	bus.mu.Lock()
	defer bus.mu.Unlock()

	var query Q
	qType := reflect.TypeOf(query)
	if qType == nil {
		panic("cannot register query handler for nil or interface type")
	}

	if _, exists := bus.handlers[qType]; exists {
		panic(fmt.Sprintf("handler already registered for query %v", qType))
	}

	bus.handlers[qType] = handler
}

// ExecuteQuery routes a query to its registered handler synchronously and returns the typed read model.
func ExecuteQuery[Q any, R any](ctx QueryContext, bus *QueryBus, query Q) (R, error) {
	qType := reflect.TypeOf(query)

	bus.mu.RLock()
	h, ok := bus.handlers[qType]
	bus.mu.RUnlock()

	var zero R
	if !ok {
		return zero, fmt.Errorf("no handler registered for query %v", qType)
	}

	handler, ok := h.(QueryHandler[Q, R])
	if !ok {
		return zero, fmt.Errorf("handler for query %v does not return the requested type", qType)
	}

	return handler.Handle(ctx, query)
}
