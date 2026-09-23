# Core API Reference

While `flux` aims to keep your domain logic completely framework-agnostic, interacting with the infrastructure layers requires understanding a few core API contracts.

## Sentinel Errors

The framework exports standard sentinel errors to allow consumers to programmatically evaluate failure reasons using `errors.Is()`.

```go
var (
	// ErrAggregateNotFound is returned when an aggregate cannot be loaded from the EventStore or SnapshotStore.
	ErrAggregateNotFound = errors.New("aggregate not found")

	// ErrSnapshotNotFound is returned by a SnapshotStore when no snapshot exists for a stream.
	ErrSnapshotNotFound = errors.New("snapshot not found")

	// ErrConcurrency is returned by an EventStore when an optimistic concurrency check fails.
	ErrConcurrency = errors.New("optimistic concurrency check failed")

	// ErrInvalidEvent is returned when an aggregate's FromEvents encounters an event type it cannot apply.
	ErrInvalidEvent = errors.New("invalid event type for aggregate")

	// ErrNoHandler is returned by Command and Query buses when no handler is registered for a given type.
	ErrNoHandler = errors.New("no handler registered")

	// ErrInvalidHandlerType is returned when a requested handler signature does not match the registered handler.
	ErrInvalidHandlerType = errors.New("invalid handler type")
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

func RegisterPointerType[T any, PT interface {
	*T
	flux.Event
}](t *Types)

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

### Protocol Buffers Codec

```go
// package protobuf ("github.com/wotek/flux/codec/protobuf")

var ErrNotProtoMessage = errors.New("event does not implement proto.Message")

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

## Storage Backends

### Redis Event Store

```go
// package redis ("github.com/wotek/flux/event/store/redis")

type Option func(*config)

func WithKeyPrefix(prefix string) Option

func WithBatchSize(size int64) Option

type EventStore struct {
	client       redis.UniversalClient
	serializer   codec.Serializer
	config       config
	appendScript *redis.Script
}

func New(client redis.UniversalClient, serializer codec.Serializer, opts ...Option) *EventStore

func NewEventStore(client redis.UniversalClient, serializer codec.Serializer, opts ...Option) *EventStore

func (s *EventStore) Append(ctx context.Context, stream flux.Stream, expectedRevision uint64, events []flux.Envelope) error

func (s *EventStore) Read(ctx context.Context, stream flux.Stream, fromRevision uint64) (flux.StreamIterator, error)

func (s *EventStore) Stream(ctx context.Context, position uint64) (flux.StreamIterator, error)
```

### Redis Snapshot Store

```go
// package redis ("github.com/wotek/flux/snapshot/store/redis")

type Option func(*config)

func WithKeyPrefix(prefix string) Option

type SnapshotStore[S any] struct {
	client redis.UniversalClient
	config config
}

func New[S any](client redis.UniversalClient, opts ...Option) *SnapshotStore[S]

func NewSnapshotStore[S any](client redis.UniversalClient, opts ...Option) *SnapshotStore[S]

func (s *SnapshotStore[S]) Load(ctx context.Context, stream flux.Stream) (flux.Snapshot[S], error)

func (s *SnapshotStore[S]) Save(ctx context.Context, stream flux.Stream, snap flux.Snapshot[S]) error
```

### MySQL Event Store

```go
// package mysql ("github.com/wotek/flux/event/store/mysql")

type Option func(*config)

func WithTableName(name string) Option

type EventStore struct {
	db         *sql.DB
	serializer codec.Serializer
	config     config
}

func New(db *sql.DB, serializer codec.Serializer, opts ...Option) *EventStore

func NewEventStore(db *sql.DB, serializer codec.Serializer, opts ...Option) *EventStore

func (s *EventStore) Append(ctx context.Context, stream flux.Stream, expectedRevision uint64, events []flux.Envelope) error

func (s *EventStore) Read(ctx context.Context, stream flux.Stream, fromRevision uint64) (flux.StreamIterator, error)

func (s *EventStore) Stream(ctx context.Context, position uint64) (flux.StreamIterator, error)
```

### MySQL Snapshot Store

```go
// package mysql ("github.com/wotek/flux/snapshot/store/mysql")

type Option func(*config)

func WithTableName(name string) Option

type SnapshotStore[S any] struct {
	db     *sql.DB
	config config
}

func New[S any](db *sql.DB, opts ...Option) *SnapshotStore[S]

func NewSnapshotStore[S any](db *sql.DB, opts ...Option) *SnapshotStore[S]

func (s *SnapshotStore[S]) Load(ctx context.Context, stream flux.Stream) (flux.Snapshot[S], error)

func (s *SnapshotStore[S]) Save(ctx context.Context, stream flux.Stream, snap flux.Snapshot[S]) error
```
