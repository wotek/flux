package query

import (
	"github.com/wotek/flux"

	"fmt"
	"reflect"
	"sync"
)

// Handler represents a component that handles a specific query and returns a read model.
type Handler[Q any, R any] interface {
	Handle(ctx Context, query Q) (R, error)
}

// Bus manages the registration and routing of queries.
type Bus struct {
	mu       sync.RWMutex
	handlers map[reflect.Type]any
}

// New creates a new QueryBus instance.
func New() *Bus {
	return &Bus{
		handlers: make(map[reflect.Type]any),
	}
}

// NewBus is an alias for New to maintain explicit constructor naming.
// RegisterHandler registers a strongly-typed handler for a specific query type.
func RegisterHandler[Q any, R any](bus *Bus, handler Handler[Q, R]) {
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

// Register registers a functional handler for a specific query type.
func Register[Q any, R any](bus *Bus, handler func(ctx Context, query Q) (R, error)) {
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

	bus.handlers[qType] = queryFuncHandler[Q, R]{fn: handler}
}

type queryFuncHandler[Q any, R any] struct {
	fn func(ctx Context, query Q) (R, error)
}

func (h queryFuncHandler[Q, R]) Handle(ctx Context, query Q) (R, error) {
	return h.fn(ctx, query)
}

// Execute routes a query to its registered handler synchronously and returns the typed read model.
func Execute[Q any, R any](ctx Context, bus *Bus, query Q) (R, error) {
	qType := reflect.TypeOf(query)

	bus.mu.RLock()
	h, ok := bus.handlers[qType]
	bus.mu.RUnlock()

	var zero R
	if !ok {
		return zero, fmt.Errorf("%w: query %v", flux.ErrNoHandler, qType)
	}

	handler, ok := h.(Handler[Q, R])
	if !ok {
		return zero, fmt.Errorf("handler for query %v does not return the requested type", qType)
	}

	return handler.Handle(ctx, query)
}
