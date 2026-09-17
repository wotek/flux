package flux

import (
	"fmt"
	"sync"
)

// EventHandler represents a component that reacts to a domain event.
type EventHandler[E Event] interface {
	Handle(ctx EventContext, event E) error
}

// EventBus manages the registration and routing of events.
// Unlike Commands/Queries, a single Event can have multiple handlers.
type EventBus struct {
	mu       sync.RWMutex
	handlers map[string][]any // maps Event.Name() to slice of handlers
}

// NewEventBus creates a new EventBus instance.
func NewEventBus() *EventBus {
	return &EventBus{
		handlers: make(map[string][]any),
	}
}

// RegisterEventHandler registers a strongly-typed handler for a specific event type.
func RegisterEventHandler[E Event](bus *EventBus, handler EventHandler[E]) {
	bus.mu.Lock()
	defer bus.mu.Unlock()

	var event E
	name := event.Name()

	wrapper := func(ctx EventContext, rawEvent any) error {
		return handler.Handle(ctx, rawEvent.(E))
	}

	bus.handlers[name] = append(bus.handlers[name], wrapper)
}

// PublishEnvelope takes a raw Envelope, constructs an EventContext, and routes the inner
// strongly-typed Event to all registered subscribers.
func PublishEnvelope(ctx EventContext, bus *EventBus, env Envelope) error {
	name := env.Event.Name()

	bus.mu.RLock()
	handlers, ok := bus.handlers[name]
	bus.mu.RUnlock()

	if !ok || len(handlers) == 0 {
		return nil // No handlers registered, that's fine for events.
	}

	var errs []error
	for _, h := range handlers {
		// Because we store closures of type untypedEventHandler, we can safely
		// assert and execute directly without reflection.
		wrapper := h.(func(EventContext, any) error)
		if err := wrapper(ctx, env.Event); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("encountered %d errors while handling event %s", len(errs), name)
	}

	return nil
}

// PublishEvent distributes a raw event to all registered handlers.
// Mostly used for internal framework events or testing.
func PublishEvent[E Event](ctx EventContext, bus *EventBus, event E) error {
	env := Envelope{Event: event}
	return PublishEnvelope(ctx, bus, env)
}
