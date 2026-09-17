package command

import (
	"github.com/wotek/flux"

	"fmt"
	"reflect"
	"sync"
)

// Handler represents a component that handles a specific command.
type Handler[C any] interface {
	Handle(ctx Context, cmd C) error
}

// Bus manages the registration and routing of commands.
type Bus struct {
	mu       sync.RWMutex
	handlers map[reflect.Type]any
}

// New creates a new command Bus instance.
func New() *Bus {
	return &Bus{
		handlers: make(map[reflect.Type]any),
	}
}

// NewBus is an alias for New to maintain explicit constructor naming.
func NewBus() *Bus {
	return New()
}

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

// RegisterCommand is an alias for Register.
func RegisterCommand[C any](bus *Bus, handler func(ctx Context, cmd C) error) {
	Register(bus, handler)
}

// RegisterCommandHandler is an alias for RegisterHandler.
func RegisterCommandHandler[C any](bus *Bus, handler Handler[C]) {
	RegisterHandler(bus, handler)
}

// Execute routes a command to its registered handler synchronously.
func Execute[C any](ctx Context, bus *Bus, cmd C) error {
	cmdType := reflect.TypeOf(cmd)

	bus.mu.RLock()
	h, ok := bus.handlers[cmdType]
	bus.mu.RUnlock()

	if !ok {
		return fmt.Errorf("%w: command %v", flux.ErrNoHandler, cmdType)
	}

	// 100% reflection-free O(1) execution via closure assertion
	wrapper := h.(func(Context, any) error)
	return wrapper(ctx, cmd)
}

// ExecuteCommand is an alias for Execute.
func ExecuteCommand[C any](ctx Context, bus *Bus, cmd C) error {
	return Execute(ctx, bus, cmd)
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
				fmt.Printf("panic executing async command %T: %v\n", cmd, r)
			}
		}()
		_ = Execute(ctx, bus, cmd)
	}()

	return nil
}

// ExecuteCommandAsync is an alias for ExecuteAsync.
func ExecuteCommandAsync[C any](ctx Context, bus *Bus, cmd C) error {
	return ExecuteAsync(ctx, bus, cmd)
}
