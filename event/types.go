package event

import (
	"errors"
	"reflect"
	"fmt"
	"sync"

	"github.com/wotek/flux"
)

var (
	// ErrTypeNotRegistered is returned when an event type has not been registered with the type registry.
	ErrTypeNotRegistered = errors.New("event type not registered")
	// ErrPointerRegistration is thrown as a panic when a pointer type is passed to RegisterType.
	ErrPointerRegistration = errors.New("RegisterType must be called with a value type, not a pointer. Use RegisterPointerType instead")
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
	if typ := reflect.TypeOf(zero); typ != nil && typ.Kind() == reflect.Pointer {
		panic(fmt.Errorf("%w (got %T)", ErrPointerRegistration, zero))
	}
	name := zero.Name()

	t.Register(name, func() flux.Event {
		// new(T) creates a pointer to the struct so unmarshalers can populate it.
		// We cast through 'any' because Go's generic type system cannot statically
		// guarantee that *T implements flux.Event, even though we know T does.
		return any(new(T)).(flux.Event)
	})
}

// RegisterPointerType uses generics to register an event type whose pointer receiver *T implements [flux.Event].
// This is particularly useful for Protocol Buffers and types with internal mutexes.
func RegisterPointerType[T any, PT interface {
	*T
	flux.Event
}](t *Types) {
	var zero PT = new(T)
	name := zero.Name()

	t.Register(name, func() flux.Event {
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
