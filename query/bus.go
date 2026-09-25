package query

import (
	"github.com/wotek/flux"

	"fmt"
	"reflect"
	"slices"
	"sync"
)

// Handler represents a component that handles a specific query and returns a read model.
type Handler[Q any, R any] interface {
	Handle(ctx Context, query Q) (R, error)
}

// Bus manages the registration and routing of queries.
type Bus struct {
	mu          sync.RWMutex
	handlers    map[reflect.Type]any
	middlewares []Middleware
}

type Middleware func(ctx Context, query any, next func(Context, any) (any, error)) (any, error)

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

func (b *Bus) Use(middlewares ...Middleware) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.middlewares = append(b.middlewares, middlewares...)
}

// Execute routes a query to its registered handler synchronously and returns the typed read model.
func Execute[Q any, R any](ctx Context, bus *Bus, query Q) (R, error) {
	qType := reflect.TypeOf(query)

	bus.mu.RLock()
	handler, ok := bus.handlers[qType]
	middlewares := slices.Clone(bus.middlewares)
	bus.mu.RUnlock()

	if !ok {
		var zero R
		return zero, fmt.Errorf("%w: query %v", flux.ErrNoHandler, qType)
	}

	exec := func(execCtx Context, execQ any) (any, error) {
		h, ok := handler.(Handler[Q, R])
		if !ok {
			return nil, fmt.Errorf("%w: handler for query %v does not return the requested type", flux.ErrInvalidHandlerType, qType)
		}
		typedQ, ok := execQ.(Q)
		if !ok {
			return nil, fmt.Errorf("%w: expected query of type %T, got %T", flux.ErrInvalidHandlerType, query, execQ)
		}
		return h.Handle(execCtx, typedQ)
	}

	for _, mw := range slices.Backward(middlewares) {
		next := exec
		exec = func(execCtx Context, execQ any) (any, error) {
			return mw(execCtx, execQ, next)
		}
	}

	res, err := exec(ctx, query)
	if res == nil {
		var zero R
		return zero, err
	}
	typedRes, ok := res.(R)
	if !ok {
		var zero R
		return zero, fmt.Errorf("%w: expected query result of type %T, got %T", flux.ErrInvalidHandlerType, zero, res)
	}
	return typedRes, err
}
