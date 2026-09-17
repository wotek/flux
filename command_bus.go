package flux

import (
	"fmt"
	"reflect"
	"sync"
)

// CommandHandler represents a component that handles a specific command.
type CommandHandler[C any] interface {
	Handle(ctx CommandContext, cmd C) error
}

// CommandBus manages the registration and routing of commands.
type CommandBus struct {
	mu       sync.RWMutex
	handlers map[reflect.Type]any
}

// NewCommandBus creates a new CommandBus instance.
func NewCommandBus() *CommandBus {
	return &CommandBus{
		handlers: make(map[reflect.Type]any),
	}
}

// RegisterCommandHandler registers a strongly-typed handler for a specific command type.
func RegisterCommandHandler[C any](bus *CommandBus, handler CommandHandler[C]) {
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

	wrapper := func(ctx CommandContext, rawCmd any) error {
		return handler.Handle(ctx, rawCmd.(C))
	}

	bus.handlers[cmdType] = wrapper
}

// ExecuteCommand routes a command to its registered handler synchronously.
func ExecuteCommand[C any](ctx CommandContext, bus *CommandBus, cmd C) error {
	cmdType := reflect.TypeOf(cmd)

	bus.mu.RLock()
	h, ok := bus.handlers[cmdType]
	bus.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no handler registered for command %v", cmdType)
	}

	// Because we store closures of type untypedCommandHandler, we can safely
	// assert and execute directly, achieving 100% reflection-free O(1) execution.
	wrapper := h.(func(CommandContext, any) error)
	return wrapper(ctx, cmd)
}

// ExecuteCommandAsync routes a command to its registered handler asynchronously.
// It fires and forgets, handling panics internally. It returns an error if routing fails.
func ExecuteCommandAsync[C any](ctx CommandContext, bus *CommandBus, cmd C) error {
	cmdType := reflect.TypeOf(cmd)

	bus.mu.RLock()
	_, ok := bus.handlers[cmdType]
	bus.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no handler registered for command %v", cmdType)
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				// In a real framework, this would plug into a logging interface.
				fmt.Printf("panic executing async command %T: %v\n", cmd, r)
			}
		}()
		_ = ExecuteCommand(ctx, bus, cmd)
	}()

	return nil
}
