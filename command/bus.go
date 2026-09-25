package command

import (
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sync"

	"github.com/wotek/flux"
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
		typedCmd, ok := rawCmd.(C)
		if !ok {
			return fmt.Errorf("%w: expected command of type %T, got %T", flux.ErrInvalidHandlerType, cmd, rawCmd)
		}
		return handler.Handle(ctx, typedCmd)
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
		typedCmd, ok := rawCmd.(C)
		if !ok {
			return fmt.Errorf("%w: expected command of type %T, got %T", flux.ErrInvalidHandlerType, cmd, rawCmd)
		}
		return handler(ctx, typedCmd)
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
	middlewares := slices.Clone(bus.middlewares)
	bus.mu.RUnlock()

	if !ok {
		return fmt.Errorf("%w: command %v", flux.ErrNoHandler, cmdType)
	}

	// 100% reflection-free O(1) execution via closure assertion
	exec, ok := h.(func(Context, any) error)
	if !ok {
		return fmt.Errorf("%w: command handler has invalid wrapper type", flux.ErrInvalidHandlerType)
	}

	for _, mw := range slices.Backward(middlewares) {
		next := exec
		exec = func(execCtx Context, execCmd any) error {
			return mw(execCtx, execCmd, next)
		}
	}

	return exec(ctx, cmd)
}

// ExecuteAsync routes a command to its registered handler asynchronously in a background goroutine.
//
// Context Semantics:
// The command executes using the provided context exactly as-is. If the caller's context is canceled
// (for example, when an HTTP request ends or a caller deadline expires), the asynchronous command
// execution will also be canceled. If callers want the command to survive the current request or scope,
// they MUST construct and pass their own detached, deadline-bound context.
//
// Guarantees & Constraints:
//   - Fire-and-Forget: ExecuteAsync returns nil as soon as the command is queued to the background
//     goroutine (or returns an error immediately if no handler is registered). It is an in-memory,
//     fire-and-forget mechanism and is lossy across application restarts or process crashes.
//   - Concurrency & Mutation: If the command is a pointer or contains mutable references, callers
//     must not mutate the command after passing it to ExecuteAsync as no deep-copy is performed.
//   - Error Handling: Execution failures and panics are logged using slog.ErrorContext.
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
				slog.ErrorContext(ctx, "panic executing async command",
					"command_type", fmt.Sprintf("%T", cmd),
					"panic", r,
				)
			}
		}()

		if err := Execute(ctx, bus, cmd); err != nil {
			slog.ErrorContext(ctx, "failed to execute async command",
				"command_type", fmt.Sprintf("%T", cmd),
				"error", err,
			)
		}
	}()

	return nil
}
