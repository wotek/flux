# Core API Reference

While `flux` aims to keep your domain logic completely framework-agnostic, interacting with the infrastructure layers requires understanding a few core API contracts.

## Sentinel Errors

The framework exports standard sentinel errors to allow consumers to programmatically evaluate failure reasons using `errors.Is()`.

```go
var (
	// ErrAggregateNotFound is returned when an aggregate cannot be loaded from the EventStore or SnapshotStore.
	ErrAggregateNotFound = errors.New("aggregate not found")

	// ErrConcurrency is returned by an EventStore when an optimistic concurrency check fails.
	ErrConcurrency = errors.New("optimistic concurrency check failed")

	// ErrInvalidEvent is returned when an aggregate's FromEvents encounters an event type it cannot apply.
	ErrInvalidEvent = errors.New("invalid event type for aggregate")

	// ErrNoHandler is returned by Command and Query buses when no handler is registered for a given type.
	ErrNoHandler = errors.New("no handler registered")
)
```

## Typed Contexts

To bridge the gap between keeping domain payloads lean and providing explicit, type-safe metadata (avoiding "magic" `context.WithValue` keys), the framework defines custom contexts for every boundary.

Because they embed standard `context.Context`, they can be passed directly into standard library functions, database queries, and the Event Store.

::: warning
Wrapping a typed context (e.g., using `context.WithTimeout`) returns a standard `context.Context`, stripping the typed methods. Handlers should extract needed metadata early if they plan to wrap the context for downstream calls.
:::

### Base Context
Provides guaranteed access to cross-cutting metadata.
```go
// package flux
type Context interface {
	context.Context
	Actor() Actor
	CorrelationIdentifier() Identifier
	CausationIdentifier() Identifier
	Logger() *slog.Logger
}
```

### Command Context
Extends the base context specifically for mutations.
```go
// package command
type Context interface {
	flux.Context
	CommandIdentifier() flux.Identifier
}
```

### Event Context
Provides strongly-typed access to the `Envelope` metadata (stream, revision, global position) while keeping your event structs pure.
```go
// package event
type Context interface {
	flux.Context
	EventIdentifier() flux.Identifier
	Stream() flux.Stream
	Revision() uint64
	Position() uint64
	Metadata() map[string]string
}
```

## Event Type Registry

The `event` package provides a reflection-free type registry to instantiate concrete event pointers by name during deserialization.

```go
// package event

var ErrTypeNotRegistered = errors.New("event type not registered")

type Types struct {
	mu        sync.RWMutex
	factories map[string]func() flux.Event
}

func NewTypes() *Types

func (t *Types) Register(name string, factory func() flux.Event)

func RegisterType[T flux.Event](t *Types)

func (t *Types) Instantiate(name string) (flux.Event, error)
```

## Serialization Codecs

The `codec` package and subpackages provide pluggable serializers for encoding and decoding `flux.Envelope` instances while preserving domain model purity.

### Codec Interface

```go
// package codec

var (
	ErrNilEvent = errors.New("envelope event cannot be nil")
	ErrEmptyEventName = errors.New("event name cannot be empty")
)

type Serializer interface {
	Marshal(env flux.Envelope) ([]byte, error)
	Unmarshal(data []byte) (flux.Envelope, error)
}
```

### JSON Codec

```go
// package json ("github.com/wotek/flux/codec/json")

type TypeRegistry interface {
	Instantiate(name string) (flux.Event, error)
}

type Serializer struct {
	types TypeRegistry
}

func New(types TypeRegistry) *Serializer

func (s *Serializer) Marshal(env flux.Envelope) ([]byte, error)

func (s *Serializer) Unmarshal(data []byte) (flux.Envelope, error)
```

### XML Codec

```go
// package xml ("github.com/wotek/flux/codec/xml")

type TypeRegistry interface {
	Instantiate(name string) (flux.Event, error)
}

type Serializer struct {
	types TypeRegistry
}

func New(types TypeRegistry) *Serializer

func (s *Serializer) Marshal(env flux.Envelope) ([]byte, error)

func (s *Serializer) Unmarshal(data []byte) (flux.Envelope, error)
```
