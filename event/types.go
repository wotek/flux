package event

import (
	"errors"
	"fmt"
	"sync"

	"github.com/wotek/flux"
)

var (
	// ErrTypeNotRegistered is returned when an event type has not been registered with the type registry.
	ErrTypeNotRegistered = errors.New("event type not registered")
)

// Types is a reflection-free registry of domain events used to instantiate concrete event pointers during deserialization.
type Types struct {
	mu        sync.RWMutex
	factories map[string]func() flux.Event
}

// NewTypes initializes a new domain event [Types] registry.
func NewTypes() *Types {
	return &Types{
		factories: make(map[string]func() flux.Event),
	}
}

// Register adds an event factory function for the given event name to the registry.
func (t *Types) Register(name string, factory func() flux.Event) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.factories[name] = factory
}

// RegisterType uses generics to capture the concrete type T and build a factory function
// that returns a new pointer to T.
func RegisterType[T flux.Event](t *Types) {
	var zero T
	name := zero.Name()

	t.Register(name, func() flux.Event {
		// new(T) creates a pointer to the struct so unmarshalers can populate it.
		// We cast through 'any' because Go's generic type system cannot statically
		// guarantee that *T implements flux.Event, even though we know T does.
		return any(new(T)).(flux.Event)
	})
}

// Instantiate returns an empty pointer to the concrete event struct based on its name.
// Returns [ErrTypeNotRegistered] if the event type was not registered.
func (t *Types) Instantiate(name string) (flux.Event, error) {
	t.mu.RLock()
	factory, exists := t.factories[name]
	t.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrTypeNotRegistered, name)
	}
	return factory(), nil
}
