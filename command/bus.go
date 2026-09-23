package command

import (
	"github.com/wotek/flux"

	"fmt"
	"reflect"
	"slices"
	"sync"
)

// Handler represents a component that handles a specific command.
type Handler[C any] interface {
	Handle(ctx Context, cmd C) error
}

// Bus manages the registration and routing of commands.
type Bus struct {
	mu          sync.RWMutex
	handlers    map[reflect.Type]any
	middlewares []Middleware
}

type Middleware func(ctx Context, cmd any, next func(Context, any) error) error

// New creates a new command Bus instance.
func New() *Bus {
	return &Bus{
		handlers: make(map[reflect.Type]any),
	}
}

// NewBus is an alias for New to maintain explicit constructor naming.
// RegisterHandler registers a strongly-typed handler for a specific command type.
func RegisterHandler[C any](bus *Bus, handler Handler[C]) {
	bus.mu.Lock()
	defer bus.mu.Unlock()

	var cmd C
	cmdType := reflect.TypeOf(cmd)
	if cmdType == nil {
		panic("cannot register command handler for nil or interface type")
	}

	if _, exists := bus.handlers[cmdType]; exists {
		panic(fmt.Sprintf("handler already registered for command %v", cmdType))
	}

	wrapper := func(ctx Context, rawCmd any) error {
		return handler.Handle(ctx, rawCmd.(C))
	}

	bus.handlers[cmdType] = wrapper
}

// Register registers a functional handler for a specific command type.
func Register[C any](bus *Bus, handler func(ctx Context, cmd C) error) {
	bus.mu.Lock()
	defer bus.mu.Unlock()

	var cmd C
	cmdType := reflect.TypeOf(cmd)
	if cmdType == nil {
		panic("cannot register command handler for nil or interface type")
	}

	if _, exists := bus.handlers[cmdType]; exists {
		panic(fmt.Sprintf("handler already registered for command %v", cmdType))
	}

	wrapper := func(ctx Context, rawCmd any) error {
		return handler(ctx, rawCmd.(C))
	}

	bus.handlers[cmdType] = wrapper
}

func (b *Bus) Use(middlewares ...Middleware) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.middlewares = append(b.middlewares, middlewares...)
}

// Execute routes a command to its registered handler synchronously.
func Execute[C any](ctx Context, bus *Bus, cmd C) error {
	cmdType := reflect.TypeOf(cmd)

	bus.mu.RLock()
	h, ok := bus.handlers[cmdType]
	middlewares := bus.middlewares
	bus.mu.RUnlock()

	if !ok {
		return fmt.Errorf("%w: command %v", flux.ErrNoHandler, cmdType)
	}

	// 100% reflection-free O(1) execution via closure assertion
	exec := h.(func(Context, any) error)

	for _, mw := range slices.Backward(middlewares) {
		next := exec
		exec = func(execCtx Context, execCmd any) error {
			return mw(execCtx, execCmd, next)
		}
	}

	return exec(ctx, cmd)
}

// ExecuteAsync routes a command to its registered handler asynchronously (fire-and-forget).
func ExecuteAsync[C any](ctx Context, bus *Bus, cmd C) error {
	cmdType := reflect.TypeOf(cmd)

	bus.mu.RLock()
	_, ok := bus.handlers[cmdType]
	bus.mu.RUnlock()

	if !ok {
		return fmt.Errorf("%w: command %v", flux.ErrNoHandler, cmdType)
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				ctx.Logger().Error("panic executing async command", "command_type", fmt.Sprintf("%T", cmd), "panic", r)
			}
		}()
		_ = Execute(ctx, bus, cmd)
	}()

	return nil
}
