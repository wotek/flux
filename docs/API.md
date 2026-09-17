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

> [!WARNING]
> Wrapping a typed context (e.g., using `context.WithTimeout`) returns a standard `context.Context`, stripping the typed methods. Handlers should extract needed metadata early if they plan to wrap the context for downstream calls.

#### Interfaces

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

// ProjectionContext extends EventContext. It acts as a distinct type boundary 
// guaranteeing that the context is bound to the projection's active database transaction.
type ProjectionContext interface {
	EventContext
}

// SagaContext extends EventContext, giving saga handlers the ability to dispatch commands.
type SagaContext interface {
	EventContext
	
	// dispatch is unexported. It is used internally by the framework's 
	// strongly-typed EnqueueCommand helper to safely queue commands.
	dispatch(cmd any)
}

// EnqueueCommand safely queues a strongly-typed command to be dispatched.
// To prevent "dual-write" anomalies (where a command fires but the saga state fails to save), 
// the framework guarantees that enqueued commands are ONLY sent to the CommandBus 
// after the Orchestrator successfully persists the Saga's updated state.
func EnqueueCommand[C any](ctx SagaContext, cmd C)
```

#### Constructors

```go
func NewContext(parent context.Context, actor Actor, correlationId Identifier, causationId Identifier) Context
func NewCommandContext(parent context.Context, cmdId Identifier, actor Actor, correlationId Identifier, causationId Identifier) CommandContext
func NewQueryContext(parent context.Context, queryId Identifier, actor Actor, correlationId Identifier, causationId Identifier) QueryContext
func NewEventContext(parent context.Context, env Envelope) EventContext
func NewProjectionContext(parent EventContext) ProjectionContext
func NewSagaContext(parent EventContext) SagaContext
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
// It can optionally be configured with a distributed transport (like NATS or SQS) for async execution.
type CommandBus struct {
	// internal fields
}

// NewCommandBus creates a new CommandBus.
func NewCommandBus() *CommandBus

// RegisterCommandHandler wires a command to its handler. 
// Panics if a handler is already registered for type C.
func RegisterCommandHandler[C any](bus *CommandBus, handler CommandHandler[C])

// ExecuteCommand routes the command to its registered handler synchronously.
// This is the default as most CQRS commands (e.g. from an HTTP request) 
// require immediate feedback on domain invariants.
func ExecuteCommand[C any](ctx context.Context, bus *CommandBus, cmd C) error

// ExecuteCommandAsync performs a "fire-and-forget" dispatch.
// If the bus has no distributed transport configured, it executes the handler 
// in a background goroutine (safely detaching the context). If a transport is 
// configured, it serializes and enqueues the command for background workers.
func ExecuteCommandAsync[C any](ctx context.Context, bus *CommandBus, cmd C) error
```

> [!TIP]
> The framework intentionally avoids "batch" or "multi-command" dispatch methods. Because `ExecuteCommand` is completely thread-safe, developers can use native Go primitives (like `sync.WaitGroup` or `golang.org/x/sync/errgroup`) to execute commands sequentially or in parallel. Long-running orchestrations should use Sagas instead of sequential scripts.

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

The Event Bus is typically invoked by a background worker tailing the `EventStore`'s global stream.

```go
// EventBus manages routing domain events to multiple subscribers.
type EventBus struct {
	// internal fields
}

// NewEventBus creates a new EventBus.
func NewEventBus() *EventBus

// RegisterEventHandler adds a subscriber to a specific event type.
func RegisterEventHandler[E Event](bus *EventBus, handler EventHandler[E])

// PublishEnvelope takes a raw Envelope (typically from the EventStore tailer), 
// constructs an EventContext, and routes the inner strongly-typed Event to all 
// registered subscribers concurrently.
func PublishEnvelope(ctx context.Context, bus *EventBus, env Envelope) error

// PublishEvent distributes a raw event to all registered handlers. 
// Mostly used for internal framework events or testing.
func PublishEvent[E Event](ctx context.Context, bus *EventBus, event E) error
```

### Projections

In CQRS, **Projections** (or Read Models) listen to the global event stream and build state optimized for read operations. 

A critical requirement of any projection is tracking its progress using a cursor (the global `Position` of the last processed event) so it can resume safely after a restart. The state mutation and the cursor update must be transactional to prevent data anomalies.

```go
// ProjectionStore defines the contract for persisting both the read-model data and its cursor.
// It abstracts the transactional boundaries of the underlying database (e.g., PostgreSQL, MongoDB).
type ProjectionStore interface {
	// GetPosition retrieves the last successfully processed global position for the projection.
	GetPosition(ctx context.Context, id Identifier) (uint64, error)

	// Update runs a database transaction. It provides the framework with a transactional 
	// context and executes the `mutate` closure (which contains the user's read-model logic). 
	// If the closure succeeds, the framework commits the transaction, atomically saving the 
	// read-model changes AND the new envelope.Position.
	Update(ctx context.Context, id Identifier, env Envelope, mutate func(txCtx context.Context) error) error
}

// Projector is the background worker that powers a Projection.
// It tails the EventStore.Stream() starting from the ProjectionStore.GetPosition().
type Projector struct {
	// internal fields
}

// NewProjector creates a new Projector instance.
func NewProjector(id Identifier, eventStore EventStore, projStore ProjectionStore) *Projector

// RegisterProjectionHandler wires a specific event type to the projection's logic.
// It leverages the same generic type-safety as the EventBus, but executes the handler 
// strictly within the ProjectionStore's Update() transaction boundary.
func RegisterProjectionHandler[E Event](p *Projector, handler func(ctx ProjectionContext, event E) error)

// Start begins tailing the EventStore in the background until the context is canceled.
func (p *Projector) Start(ctx context.Context) error
```

#### Cross-Domain Projections

Because the `Projector` tails the **Global Event Stream** (via `EventStore.Stream`) rather than individual aggregate streams, it natively supports listening to events across entirely different domains. You can simply register multiple event types to the same projector, and it will route them sequentially in the exact deterministic order they occurred system-wide.

```go
// Example: A single dashboard projection handling cross-domain events
proj := flux.NewProjector(dashboardID, eventStore, sqlStore)

flux.RegisterProjectionHandler(proj, func(ctx flux.EventContext, e OrderCreated) error { /* ... */ })
flux.RegisterProjectionHandler(proj, func(ctx flux.EventContext, e UserRegistered) error { /* ... */ })
```

#### Temporal Integration (Optional Path)

Because the framework strictly decouples **Routing** from **Execution**, you are not forced to use the default `Projector` or `ProjectionStore`. If you prefer to run Projections as durable [Temporal Workflows](https://temporal.io/), the framework provides the perfect hooks to bridge the gap.

Instead of a traditional pull-based projector, you can use the `EventBus` to push events directly into Temporal as Signals:

```go
// Example: Using the EventBus to bridge events into Temporal
flux.RegisterEventHandler[OrderCreated](bus, func(ctx flux.EventContext, e OrderCreated) error {
	// The EventContext provides the metadata, and 'e' is the clean payload
	return temporalClient.SignalWorkflow(
		ctx, 
		"Projection-Dashboard", // Target Temporal Workflow ID
		"", 
		"OrderCreatedSignal", 
		e,
	)
})
```

Because Temporal workflows durably persist their own local state and handle retries natively, this approach eliminates the need to manually track `Position` cursors or manage database transactions.

### Sagas / Process Managers

A **Saga** (or Process Manager) coordinates long-running business processes that span multiple aggregates. It listens to domain events, maintains internal state to track the progress of the workflow, and dispatches commands to other aggregates.

```go
// Saga defines the contract for a process manager.
type Saga interface {
	// Identifier returns the globally unique ID of this saga instance.
	// This is typically derived from the CorrelationIdentifier of the triggering event.
	Identifier() Identifier
}

// SagaStore defines how the internal state of a saga is persisted between events.
type SagaStore interface {
	Load(ctx context.Context, id Identifier, saga Saga) error
	Save(ctx context.Context, saga Saga) error
}

// Orchestrator is the background worker that listens to the global event stream 
// and routes events to the appropriate saga instances.
type Orchestrator struct {
	// internal fields
}

func NewOrchestrator(eventStore EventStore, sagaStore SagaStore, commandBus *CommandBus) *Orchestrator

// RegisterSagaHandler wires a specific event type to a saga's state transition.
func RegisterSagaHandler[S Saga, E Event](o *Orchestrator, handler func(ctx SagaContext, saga S, event E) error)
```

#### Usage Example: The Outbox Pattern

To prevent "dual-write" anomalies (where a command executes successfully but the Saga fails to save its state, causing the command to be duplicated on retry), Sagas do **not** execute commands synchronously. Instead, they use `EnqueueCommand`. The framework handles the transactional safety automatically.

```go
// Example: A Saga handling user onboarding
flux.RegisterSagaHandler(orchestrator, func(ctx flux.SagaContext, saga *OnboardingSaga, e UserRegistered) error {
	// 1. Update internal saga state based on the event
	saga.ID = ctx.CorrelationIdentifier()
	saga.Status = "AWAITING_WELCOME_EMAIL"

	// 2. Safely queue a strongly-typed command
	// The Orchestrator will automatically persist the saga state to the SagaStore 
	// before actually routing this command to the CommandBus.
	flux.EnqueueCommand(ctx, SendWelcomeEmail{Email: e.Email})

	return nil // Returning nil tells the Orchestrator to commit state and fire queued commands
})
```

#### Temporal Integration (Optional Path)

Because Sagas are inherently stateful and frequently require timers (e.g., "if payment isn't confirmed in 10 minutes, issue a refund command"), they are notoriously complex to build in vanilla databases. This makes them the absolute **perfect candidate** for Temporal workflows.

If you choose to run your Sagas in Temporal, you do not need the `SagaStore` or `Orchestrator`. 
Instead:
1. You use the `EventBus` to push events into a Temporal Workflow (just like Projections).
2. The Temporal Workflow *is* your Saga. It natively maintains its own local state variables.
3. When the Workflow wants to dispatch a command, it executes a Temporal `Activity` that calls `flux.ExecuteCommandAsync(ctx, bus, myCmd)`.
