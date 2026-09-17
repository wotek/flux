# API Design / Specification

This document tracks the objects, structs, interfaces, and methods for the event sourcing framework.

## Concepts

### Stream

A stream represents an append-only log of events that belong to a specific entity or aggregate. It tracks the identity of the sequence.

```go
package flux // TBD package name

// Stream represents a sequence of events for a specific aggregate or entity.
type Stream struct {
	// Identifier is the unique identifier of the stream.
	Identifier Identifier
}
```

### Identifier

The identifier provides a unified and globally unique way to address any resource (streams, events, actors, projections, sagas) within the system.

Format: `urn:<organization>:<environment>:<service>:<account_id>:<resource_type>[:/]<resource_id>[@<version>]`

*   `organization`: Top-level boundary.
*   `environment`: Optional (e.g., `eu-west-1`, `production`).
*   `service`: The service namespace (e.g., `auth`, `payments`, `inventory`).
*   `account_id`: Customer, workspace, or org ID.
*   `resource_type`: The type of resource (e.g., `user`, `stream`, `event`).
*   `resource_id`: The identifier or path to the resource. Separated from `resource_type` by `:` (for simple IDs) or `/` (for paths).
*   `version`: Optional annotation (prefixed with `@`) to specify a version, which is particularly useful in an event-sourced system.

```go
// Component represents a part of the Identifier.
type Component int

const (
	ComponentOrganization Component = iota
	ComponentEnvironment
	ComponentService
	ComponentAccountID
	ComponentResourceType
	ComponentResourceID
	ComponentVersion
)

// Identifier represents a globally unique resource identifier.
// It is stored as a single underlying URN string to minimize memory footprint (16 bytes)
// when passed by value across envelopes and messages.
type Identifier struct {
	urn string
}

// Component accessors extract the respective parts from the underlying URN string lazily.
func (i Identifier) Organization() string 
func (i Identifier) Environment() string
func (i Identifier) Service() string
func (i Identifier) AccountID() string
func (i Identifier) ResourceType() string
func (i Identifier) ResourceID() string
func (i Identifier) Version() string // Returns string to support "1", "latest", or UUIDs

// IsEmpty returns true if the Identifier is the zero value (uninitialized or empty).
func (i Identifier) IsEmpty() bool

// String returns the canonical string representation of the Identifier.
// It consistently uses ':' to separate all components, including ResourceType
// and ResourceID, and allows ResourceID to natively contain path separators ('/').
// Empty optional components (like Environment) are represented by empty strings between colons.
// It appends '@version' if Version is not empty.
func (i Identifier) String() string

// ParseIdentifier parses a formatted URN string into an Identifier struct.
func ParseIdentifier(s string) (Identifier, error)

// Is checks if a specific component of the identifier matches the given value.
// E.g., id.Is(ComponentService, "payments")
func (i Identifier) Is(c Component, value string) bool
```

### Event and Envelope

In event sourcing, an **Event** represents a domain fact (something that happened), while an **Envelope** wraps that event with essential framework metadata (such as timestamps, positions, actors, and causal context).

```go
// Event represents a strongly-typed domain event payload.
// This is an interface that concrete event types (e.g., UserCreated) implement.
type Event interface {
	// Name returns the string representation of the event type.
	Name() string
}

// Envelope wraps a domain event with standard framework metadata.
type Envelope struct {
	// Identifier is the globally unique identifier of this specific event occurrence.
	// e.g., urn:myorg:prod:payments:tenant1:event/uuid-1234
	Identifier Identifier

	// Stream is the stream this event belongs to.
	Stream Stream

	// Revision is the sequence number of this event within its specific stream.
	Revision uint64

	// Position is the sequence number of this event in the Global Event Stream.
	// This is useful for iterating over events across all streams in the system.
	Position uint64

	// Event is the actual strongly-typed domain payload.
	Event Event

	// Metadata contains optional, custom headers for the event (e.g., tracing spans, feature flags).
	// To minimize heap allocations during large reads, this map should remain nil if empty.
	Metadata map[string]string

	// CreatedAt is the time the event was generated.
	CreatedAt time.Time

	// Actor represents the user, system, or service that caused this event.
	Actor Actor

	// CorrelationIdentifier ties this event to a specific command, request, or overarching transaction.
	CorrelationIdentifier Identifier

	// CausationIdentifier points to the ID of the specific event or command that directly caused this event.
	CausationIdentifier Identifier
}
```

### Actor

The **Actor** represents the entity (user, service, or system process) that initiated the state change. It carries its own `Identifier` and can be expanded in the future to carry additional authorization or contextual metadata.

```go
// Actor represents the entity that initiated a change in the system.
type Actor struct {
	// Identifier is the globally unique ID of the actor (e.g., urn:...:user/123 or urn:...:service/auth).
	Identifier Identifier
	
	// Additional fields like Roles or IP address can be added here if needed for auditing.
}
```

### Event Store

The **Event Store** is the append-only database of the system. 

It is highly recommended that the Event Store deals exclusively with **Envelopes** rather than raw Events. The `Aggregate Repository` (which we will design later) will act as the bridge: it takes raw `Event`s emitted by your aggregates, wraps them in `Envelope`s (enriching them with the `Actor`, `CorrelationIdentifier`, etc., often pulled from the `context.Context`), and passes those Envelopes to the Event Store.

```go
// StreamIterator is a standardized iterator over a sequence of Envelopes.
// It is fully compatible with Go 1.23 iter.Seq2, allowing it to be used directly in for-range loops.
type StreamIterator iter.Seq2[Envelope, error]

const (
	// ExpectedRevisionAny instructs the EventStore to append events without checking the current stream revision.
	ExpectedRevisionAny uint64 = 1<<64 - 1 // math.MaxUint64

	// ExpectedRevisionNoStream instructs the EventStore to append only if the stream does not exist yet.
	ExpectedRevisionNoStream uint64 = 0
)

// EventStore defines the contract for persisting and retrieving events.
type EventStore interface {
	// Append adds one or more envelopes to a specific stream.
	// The `expectedRevision` is used for optimistic concurrency control (e.g.,
	// rejecting the append if the stream's current revision does not match expectedRevision).
	// Constants ExpectedRevisionAny and ExpectedRevisionNoStream can be used for special behavior.
	Append(ctx Context, stream Stream, expectedRevision uint64, envelopes []Envelope) error

	// Read retrieves a sequence of envelopes from a specific stream.
	// `fromRevision` dictates the starting sequence number (inclusive).
	// `limit` caps the number of events returned (0 can be used to mean no limit).
	// It returns a StreamIterator for efficient traversal.
	Read(ctx Context, stream Stream, fromRevision uint64, limit uint64) (StreamIterator, error)

	// Stream iterates over the global event stream across all aggregates.
	// It starts from a specific global Position. This is primarily used by Projections and Sagas.
	// It returns a StreamIterator for efficient, leak-free traversal.
	Stream(ctx Context, from uint64) (StreamIterator, error)
}
```

### Aggregate

The **Aggregate** (or Aggregate Root) is the primary consistency boundary in the domain. It is responsible for enforcing business invariants. Instead of mutating database rows directly, it executes business logic and emits domain **Events** to represent state changes.

Leveraging Go Generics, the framework allows each domain aggregate to enforce strict type checks on its own specific Event types (e.g., an `OrderAggregate` only accepts `OrderEvent`s).

```go
// Changeset tracks uncommitted events generated during command execution.
// The type parameter E ensures only events belonging to this aggregate are recorded.
type Changeset[E Event] interface {
	// Record adds a new event to the changeset.
	Record(event E)
	
	// Clear empties the changeset (called after successful persistence).
	Clear()
	
	// HasChanges returns true if there are uncommitted events.
	HasChanges() bool
	
	// Events returns the list of uncommitted events.
	Events() []E
}

// NewChangeset provides a default, slice-backed implementation of the Changeset interface.
func NewChangeset[E Event]() Changeset[E]

// Aggregate defines the core contract for a domain aggregate.
type Aggregate[E Event] interface {
	// Identifier returns the globally unique identifier for this aggregate.
	Identifier() Identifier

	// Changeset returns the tracker for new, uncommitted events.
	Changeset() Changeset[E]

	// FromEvents replays historical events to reconstitute the aggregate's state.
	// It accepts a StreamIterator (which yields Envelopes) to allow the aggregate 
	// to synchronize its internal Revision alongside applying the event payloads.
	FromEvents(events StreamIterator) error
}

// AggregateRoot is an embeddable struct providing the foundational boilerplate 
// for any domain aggregate (composition over inheritance).
type AggregateRoot[E Event] struct {
	id        Identifier
	revision  uint64
	changeset Changeset[E]
	
	// apply is a closure/method provided by the concrete aggregate to mutate its state.
	apply func(E) error
}

// NewAggregateRoot initializes the boilerplate. The concrete aggregate passes its Apply method.
// Note: Revisions are not incremented when recording new events to the changeset, only when 
// replaying from the EventStore or after successful persistence.
func NewAggregateRoot[E Event](id Identifier, changeset Changeset[E], apply func(E) error) AggregateRoot[E] {
	return AggregateRoot[E]{
		id:        id,
		changeset: changeset,
		apply:     apply,
	}
}

func (a *AggregateRoot[E]) Identifier() Identifier { return a.id }
func (a *AggregateRoot[E]) Changeset() Changeset[E] { return a.changeset }
func (a *AggregateRoot[E]) Revision() uint64 { return a.revision }

// FromEvents iterates over the StreamIterator, type-asserts the generic Event 
// into the aggregate's specific Event type E, applies it, and updates the revision.
func (a *AggregateRoot[E]) FromEvents(events StreamIterator) error {
	for env, err := range events {
		if err != nil {
			return err
		}
		
		// Ensure the event conforms to this aggregate's specific event type constraint.
		domainEvent, ok := env.Event.(E)
		if !ok {
			return fmt.Errorf("aggregate %s cannot apply event of type %T", a.id, env.Event)
		}

		if err := a.apply(domainEvent); err != nil {
			return err
		}
		// Synchronize aggregate revision with the envelope's revision
		a.revision = env.Revision
	}
	return nil
}
```

### Aggregate Repository

The **Aggregate Repository** acts as the bridge between the domain Aggregates and the framework's `EventStore`. 

It is responsible for:
1. **Loading**: Fetching raw envelopes from the `EventStore` and calling `FromEvents` on the user-provided aggregate instance to reconstitute its state.
2. **Saving**: Taking the uncommitted events from the aggregate's `Changeset`, wrapping them in `Envelope`s (attaching the provided Actor), appending them to the `EventStore`, and finally clearing the changeset.

```go
// AggregateRepository manages the loading and saving of domain aggregates.
// It is implemented as a concrete struct rather than an interface, aligning with Go's 
// "accept interfaces, return structs" philosophy. Consumers can mock it natively.
type AggregateRepository[A Aggregate[E], E Event] struct {
	eventStore EventStore
}

// NewAggregateRepository creates a new repository instance.
func NewAggregateRepository[A Aggregate[E], E Event](store EventStore) *AggregateRepository[A, E] {
	return &AggregateRepository[A, E]{
		eventStore: store,
	}
}

// Load fetches events for the provided aggregate from the Event Store and replays them.
// It uses the "Pass-by-Pointer" pattern: the consumer instantiates the empty aggregate 
// and passes it in. The repository uses `aggregate.Identifier()` to fetch the correct stream.
// If the stream does not exist, it typically returns an error (e.g., ErrNotFound).
func (r *AggregateRepository[A, E]) Load(ctx Context, aggregate A) error

// Save persists the uncommitted events from the aggregate's Changeset to the Event Store.
// It is responsible for generating globally unique Identifiers for each new event occurrence,
// wrapping the raw domain Event payloads into Envelopes (pulling Actor and CorrelationIdentifier from the Context),
// and calling the EventStore.Append method with the aggregate's current revision for concurrency control.
// After successful persistence, it calls Clear() on the Changeset.
func (r *AggregateRepository[A, E]) Save(ctx Context, aggregate A) error
}
```

### Snapshotting (Future Phase)

For long-lived aggregates that accumulate thousands of events over time, replaying the entire stream from `version 0` during a `Load` operation can become a performance bottleneck. 

To mitigate this, a future phase of the framework will introduce **Snapshotting**. This will likely involve a `SnapshotStore` and an optional `Snapshotable` interface on the Aggregate that allows the Repository to load state from the most recent snapshot and only replay events that occurred *after* the snapshot's version.

### Message Bus / Dispatcher

The framework utilizes three distinct buses to implement CQRS (Command Query Responsibility Segregation) and Event-Driven architecture:
1. **Command Bus**: Routes a Command to exactly *one* Command Handler.
2. **Query Bus**: Routes a Query to exactly *one* Query Handler, returning a strongly-typed result.
3. **Event Bus**: Routes an Event to *zero or more* Event Handlers (used for projections, side-effects, and sagas).

By leveraging Go Generics and package-level execution functions, we achieve 100% type safety on inputs and outputs without forcing Commands or Queries to implement marker interfaces.

### Contexts

To bridge the gap between keeping domain payloads lean and providing explicit, type-safe metadata (avoiding "magic" context keys), the framework defines custom contexts. 

Because they embed `context.Context`, they can be passed directly into standard library functions, database queries, and the Event Store.

```go
// Context is the base typed context for the framework, providing guaranteed
// access to metadata for any operation.
type Context interface {
	context.Context
	Actor() Actor
	CorrelationIdentifier() Identifier
	CausationIdentifier() Identifier
}

// CommandContext extends the base Context for command execution.
type CommandContext interface {
	Context
	CommandIdentifier() Identifier
}

// QueryContext extends the base Context for read-only query operations.
type QueryContext interface {
	Context
	QueryIdentifier() Identifier
}

// EventContext provides strongly-typed access to the Envelope metadata 
// while keeping the event payload clean.
type EventContext interface {
	Context
	EventIdentifier() Identifier
	Stream() Stream
	Revision() uint64
	Position() uint64
	Metadata() map[string]string
}
```

### Handlers

Handlers define the interface for processing Commands, Queries, and Events. They receive their respective strongly-typed contexts.

```go
// CommandHandler executes business logic for a specific command.
type CommandHandler[C any] interface {
	Handle(ctx CommandContext, cmd C) error
}

// QueryHandler executes read-only logic and returns a strongly-typed result.
type QueryHandler[Q any, R any] interface {
	Handle(ctx QueryContext, query Q) (R, error)
}

// EventHandler reacts to domain events that were successfully persisted to the Event Store.
type EventHandler[E Event] interface {
	Handle(ctx EventContext, event E) error
}
```

### Command Bus

```go
// CommandBus manages command routing.
type CommandBus struct {
	// internal fields
}

// NewCommandBus creates a new CommandBus.
func NewCommandBus() *CommandBus

// RegisterCommandHandler wires a command to its handler. 
// Panics if a handler is already registered for type C.
func RegisterCommandHandler[C any](bus *CommandBus, handler CommandHandler[C])

// ExecuteCommand routes the command to its registered handler.
func ExecuteCommand[C any](ctx context.Context, bus *CommandBus, cmd C) error
```

### Query Bus

```go
// QueryBus manages query routing.
type QueryBus struct {
	// internal fields
}

// NewQueryBus creates a new QueryBus.
func NewQueryBus() *QueryBus

// RegisterQueryHandler wires a query to its handler and expected return type.
// Panics if a handler is already registered for type Q.
func RegisterQueryHandler[Q any, R any](bus *QueryBus, handler QueryHandler[Q, R])

// ExecuteQuery routes the query to its registered handler, returning the strongly-typed result R.
func ExecuteQuery[Q any, R any](ctx context.Context, bus *QueryBus, query Q) (R, error)
```

### Event Bus

```go
// EventBus manages routing domain events to multiple subscribers.
type EventBus struct {
	// internal fields
}

// NewEventBus creates a new EventBus.
func NewEventBus() *EventBus

// RegisterEventHandler adds a subscriber to a specific event type.
func RegisterEventHandler[E Event](bus *EventBus, handler EventHandler[E])

// PublishEvent distributes the event to all registered handlers concurrently.
func PublishEvent[E Event](ctx context.Context, bus *EventBus, event E) error
```
