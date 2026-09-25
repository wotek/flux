package event

import (
	"errors"
	"slices"
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
	mu          sync.RWMutex
	handlers    map[string][]any // maps Event.Name() to slice of handlers
	middlewares []Middleware
}

type Middleware func(ctx Context, env flux.Envelope, next func(Context, flux.Envelope) error) error

// New creates a new event Bus instance.
func New() *Bus {
	return &Bus{
		handlers: make(map[string][]any),
	}
}

// NewBus is an alias for New to maintain explicit constructor naming.
func NewBus() *Bus {
	return New()
}

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
func (b *Bus) Use(middlewares ...Middleware) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.middlewares = append(b.middlewares, middlewares...)
}

func PublishEnvelope(ctx Context, bus *Bus, env flux.Envelope) error {
	name := env.Event.Name()

	bus.mu.RLock()
	handlers := slices.Clone(bus.handlers[name])
	middlewares := slices.Clone(bus.middlewares)
	bus.mu.RUnlock()

	if len(handlers) == 0 {
		return nil
	}

	exec := func(execCtx Context, execEnv flux.Envelope) error {
		var errs []error
		for _, h := range handlers {
			wrapper := h.(func(Context, any) error)
			if err := wrapper(execCtx, execEnv.Event); err != nil {
				errs = append(errs, err)
			}
		}

		if len(errs) > 0 {
			return errors.Join(errs...)
		}
		return nil
	}

	for _, mw := range slices.Backward(middlewares) {
		next := exec
		exec = func(execCtx Context, execEnv flux.Envelope) error {
			return mw(execCtx, execEnv, next)
		}
	}

	return exec(ctx, env)
}

// Publish wraps a domain event in an Envelope and routes it to all registered subscribers.
func Publish[E flux.Event](ctx Context, bus *Bus, event E) error {
	env := flux.Envelope{Event: event}
	return PublishEnvelope(ctx, bus, env)
}
