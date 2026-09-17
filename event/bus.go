package event

import (
	"fmt"
	"sync"

	"github.com/wotek/flux"
)

// Handler represents a component that reacts to a domain event.
type Handler[E flux.Event] interface {
	Handle(ctx Context, event E) error
}

// Bus manages the registration and routing of events.
// A single event type can have multiple subscribers.
type Bus struct {
	mu       sync.RWMutex
	handlers map[string][]any // maps Event.Name() to slice of handlers
}

// New creates a new event Bus instance.
func New() *Bus {
	return &Bus{
		handlers: make(map[string][]any),
	}
}

// NewBus is an alias for New to maintain explicit constructor naming.
// RegisterHandler registers a strongly-typed handler for a specific event type.
func RegisterHandler[E flux.Event](bus *Bus, handler Handler[E]) {
	bus.mu.Lock()
	defer bus.mu.Unlock()

	var event E
	name := event.Name()

	wrapper := func(ctx Context, rawEvent any) error {
		return handler.Handle(ctx, rawEvent.(E))
	}

	bus.handlers[name] = append(bus.handlers[name], wrapper)
}

// Register registers a functional handler for a specific event type.
func Register[E flux.Event](bus *Bus, handler func(ctx Context, event E) error) {
	bus.mu.Lock()
	defer bus.mu.Unlock()

	var event E
	name := event.Name()

	wrapper := func(ctx Context, rawEvent any) error {
		return handler(ctx, rawEvent.(E))
	}

	bus.handlers[name] = append(bus.handlers[name], wrapper)
}

// PublishEnvelope routes an Envelope to all registered subscribers.
func PublishEnvelope(ctx Context, bus *Bus, env flux.Envelope) error {
	name := env.Event.Name()

	bus.mu.RLock()
	handlers, ok := bus.handlers[name]
	bus.mu.RUnlock()

	if !ok || len(handlers) == 0 {
		return nil
	}

	var errs []error
	for _, h := range handlers {
		wrapper := h.(func(Context, any) error)
		if err := wrapper(ctx, env.Event); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("encountered %d errors while handling event %s", len(errs), name)
	}

	return nil
}

// Publish wraps a domain event in an Envelope and routes it to all registered subscribers.
func Publish[E flux.Event](ctx Context, bus *Bus, event E) error {
	env := flux.Envelope{Event: event}
	return PublishEnvelope(ctx, bus, env)
}
