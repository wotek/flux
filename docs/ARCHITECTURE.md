# Architecture, Dependency Graph & Subpackage Reference

This document provides a comprehensive overview of the `flux` framework architecture, including package dependency analysis, proof of acyclic hierarchy, and a complete catalog of all exported structs, interfaces, and functions across subpackages.

---

## 1. Dependency Graph & Cycle Analysis

The framework follows a strict **layered directed acyclic graph (DAG)** architecture. Core domain models and primitives remain self-contained at the root, while messaging buses, projections, and workflows reside in dedicated subpackages that depend unidirectionally on the core.

### Package Hierarchy Overview

```text
                                    ┌───────────────┐
                                    │  flux (core)  │
                                    │  - Primitives │
                                    │  - Context    │
                                    │  - Aggregate  │
                                    │  - Repository │
                                    │  - Snapshot   │
                                    └───────┬───────┘
          ┌─────────────────────┬───────────┼───────────┬─────────────────────┐
          │                     │           │           │                     │
          ▼                     ▼           ▼           ▼                     ▼
  ┌───────────────┐     ┌───────────────┐   │   ┌───────────────┐     ┌───────────────┐
  │    command    │     │     query     │   │   │     event     │     │  event/store  │
  │  - Bus        │     │  - Bus        │   │   │  - Bus        │     │  - Store      │
  │  - Context    │     │  - Context    │   │   │  - Context    │     └───────────────┘
  │  - Handler    │     │  - Handler    │   │   │  - Handler    │
  └───────┬───────┘     └───────────────┘   │   └───────┬───────┘
          │                                 │           │
          │             ┌───────────────────┘           ├───────────────────────────────┐
          │             │                               │ (consumes global events)      │ (consumes global events)
          │             ▼                               ▼                               ▼
          │     ┌───────────────┐               ┌───────────────┐               ┌───────────────┐
          │     │   snapshot    │               │  projection   │               │   workflow    │
          │     │  - Repository │
                                    │  - Snapshot   │               │  - Projector  │               │  - Orchestr.  │
          │     │  - Store      │               │  - Context    │               │  - Context    │
          │     │  - Schedule   │               │  - Store      │               │  - Store      │
          │     └───────────────┘               └───────┬───────┘               └───────┬───────┘
          │                                             │                               │
          │                                             ▼                               │
          │                                     ┌───────────────┐                       │
          │                                     │  projection/  │                       │
          │                                     │     store     │                       │
          │                                     │  - Store      │                       │
          │                                     └───────────────┘                       │
          │                                                                             │
          │ (dispatches outbox commands)                                                ▼
          └─────────────────────────────────────────────────────────────────────┌───────────────┐
                                                                                │workflow/store │
                                                                                │  - Store      │
                                                                                └───────────────┘
```

### Dependency Flow Diagram

```mermaid
flowchart TD
    subgraph Core ["Core Primitives"]
        Flux["flux<br/>(Identifier, Stream, Actor, Event, Envelope, AggregateRoot, AggregateRepository, Context)"]
    end

    subgraph EventStoreDomain ["Event Storage"]
        EventStore["event/store<br/>(EventStore)"]
    end

    subgraph CommandDomain ["Command Bus"]
        Command["command<br/>(Bus, Context, Handler)"]
    end

    subgraph QueryDomain ["Query Bus"]
        Query["query<br/>(Bus, Context, Handler)"]
    end

    subgraph EventDomain ["Event Bus"]
        Event["event<br/>(Bus, Context, Handler)"]
    end

    subgraph ProjectionDomain ["Projections"]
        Projection["projection<br/>(Projector, Context, Store)"]
        ProjStore["projection/store<br/>(ProjectionStore)"]
    end

    subgraph WorkflowDomain ["Workflows"]
        Workflow["workflow<br/>(Orchestrator, Context, Store)"]
        WorkflowStore["workflow/store<br/>(WorkflowStore)"]
    end

    %% Dependencies
    EventStore -->|implements flux.EventStore| Flux
    Command --> Flux
    Query --> Flux
    Event --> Flux

    Projection --> Flux
    Projection --> Event
    ProjStore --> Projection
    ProjStore --> Flux

    Workflow --> Flux
    Workflow --> Event
    WorkflowStore --> Workflow
    WorkflowStore --> Command
    WorkflowStore --> Flux
```

### Dependency Rules & Cycle Prevention

1. **Zero Downward Imports:** The root `flux` package imports **none** of the subpackages (`command`, `query`, `event`, `projection`, `workflow`, or any `store`). It can never participate in an import cycle.
2. **Context Extension Hierarchy:**
   - `flux.Context` provides base execution metadata (`Actor`, `CorrelationIdentifier`, `CausationIdentifier`).
   - `command.Context` embeds `flux.Context` and adds `CommandIdentifier()`.
   - `query.Context` embeds `flux.Context` and adds `QueryIdentifier()`.
   - `event.Context` embeds `flux.Context` and adds `EventIdentifier()`, `Stream()`, `Revision()`, `Position()`, `Metadata()`.
   - `projection.Context` embeds `event.Context`.
   - `workflow.Context` embeds `event.Context` and adds `QueuedCommands()`.
3. **Workflow Outbox Integration:** The `workflow/store` driver imports `command.Bus` to dispatch asynchronous outbox commands. Because `command` has no knowledge of `workflow`, the dependency remains strictly unidirectional (`workflow/store` $\rightarrow$ `command` $\rightarrow$ `flux`).

---

## 2. Subpackages Reference

### Package: `github.com/wotek/flux`

The core module providing foundational primitives, aggregate lifecycle management, and persistence contracts.

#### Structs

- `Identifier`: Compact, zero-allocation Uniform Resource Name (URN) addressing any resource in the format `urn:<org>:<env>:<service>:<account>:<type>:<id>[@version]`.
- `Stream`: Represents an append-only stream of events identified by an `Identifier`.
- `Actor`: Represents the user, system, or service that triggered a state change.
- `Envelope`: Wraps a domain event payload with revision, global position, actor, causation, correlation, and timestamp metadata.
- `Changeset[E Event]`: Tracks uncommitted events generated during aggregate operations.
- `AggregateRoot[E Event]`: Embeddable base for building aggregates with automatic revision tracking, event replaying, and changeset management.
  \* `AggregateRepository\[A Aggregate\[A, E\], E Event\]`: Unit-of-work repository for loading aggregates from an `EventStore` and saving uncommitted events with optimistic concurrency verification.
- `Snapshot[S any]`: Represents a captured point-in-time state of an Aggregate.
- `SnapshotRepository[A Aggregate[A, E], E Event, S any]`: Decorator wrapping `flux.AggregateRepository` that automatically handles snapshot loading and saving.

#### Interfaces

- `Event`: Marker interface implemented by domain events; requires `Name() string`.
- `Aggregate[A, E]`: Go 1.26 self-referencing generic constraint implemented by aggregate roots. Requires `Identifier() Identifier`, `Revision() uint64`, `Changeset() Changeset[E]`, `FromEvents(StreamIterator) error`, and `New(Stream) A`.
- `Changeset[E Event]`: Interface for recording and retrieving uncommitted domain events.
- `EventStore`: Persistence contract defining `Append(ctx, stream, expectedRevision, events)`, `Read(ctx, stream, fromRevision)`, and `Stream(ctx, fromPosition)`.
  \* `Context`: Base execution context providing `Actor\(\)`, `CorrelationIdentifier\(\)`, and `CausationIdentifier\(\)`.
- `Snapshotable[S any]`: Implemented by aggregates. Defines `Snapshot() S` and `With(state S)`.
- `SnapshotStore[S any]`: Persistence contract defining `Load(ctx, stream) (Snapshot[S], error)` and `Save(ctx, stream, snap) error`.
- `SnapshotSchedule[A any]`: Determines when to snapshot. Defines `Test(aggregate A) bool`.

#### Functions

- `NewIdentifier(org, env, svc, account, resType, resID, version string) Identifier`: Constructs an Identifier.
- `ParseIdentifier(s string) (Identifier, error)`: Parses an RFC-like URN into an `Identifier`.
- `MustParseIdentifier(s string) Identifier`: Parses an Identifier or panics (ideal for test setups).
- `NewChangeset[E Event]() Changeset[E]`: Constructs an in-memory changeset.
- `NewAggregateRoot[E Event](stream Stream, changeset Changeset[E], apply func(E) error) AggregateRoot[E]`: Constructs an embeddable `AggregateRoot`.
  \* `NewAggregateRepository\[A, E\]\(eventStore EventStore\) \*AggregateRepository\[A, E\]`: Creates an `AggregateRepository`.
- `NewSnapshotRepository[A Aggregate[A, E], E Event, S any](base *AggregateRepository[A, E], store SnapshotStore[S], schedule SnapshotSchedule[A], eventStore EventStore) *SnapshotRepository[A, E, S]`: Constructs the snapshot repository decorator.
- `Every[A Aggregate[A, E], E Event](n uint64) SnapshotSchedule[A]`: Creates a schedule triggering every `n` events.
- `NewContext(parent context.Context, actor Actor, correlationId Identifier, causationId Identifier) Context`: Constructs a base `flux.Context`.

---

### Package: `github.com/wotek/flux/command`

Provides in-memory, constant-time `O(1)` routing for CQRS command dispatching.

#### Structs & Types

- `Bus`: Concurrency-safe registry routing commands to single registered handlers.

#### Interfaces

- `Context`: Extends `flux.Context` with `CommandIdentifier() flux.Identifier`.
- `Handler[C any]`: Defines `Handle(ctx Context, cmd C) error` for processing command `C`.

#### Functions

- `New() *Bus`: Creates a new command bus.
- `(*Bus).Use(middlewares ...Middleware)`: Registers interceptors in the bus.
- `(*Bus).SetAsyncErrorHandler(hook AsyncErrorHandler)`: Registers a callback for asynchronous command dispatch errors.
- `NewContext(parent context.Context, cmdID flux.Identifier, actor flux.Actor, correlationID flux.Identifier, causationID flux.Identifier) Context`: Creates a command context.
- `Register[C any](bus *Bus, handler func(ctx Context, cmd C) error)`: Registers a closure handler for command type `C`.
- `RegisterHandler[C any](bus *Bus, handler Handler[C])`: Registers an interface handler for command type `C`.
- `Execute[C any](ctx Context, bus *Bus, cmd C) error`: Dispatches command `C` synchronously with `O(1)` performance.
- `ExecuteAsync[C any](ctx Context, bus *Bus, cmd C) error`: Dispatches command `C` in a background goroutine with detached cancellation context, error logging, and panic recovery.

---

### Package: `github.com/wotek/flux/query`

Provides type-safe, reflection-free query execution and read-model retrieval.

#### Structs & Types

- `Bus`: Concurrency-safe registry routing queries to their handler.

#### Interfaces

- `Context`: Extends `flux.Context` with `QueryIdentifier() flux.Identifier`.
- `Handler[Q any, R any]`: Defines `Handle(ctx Context, query Q) (R, error)`.

#### Functions

- `New() *Bus`: Creates a new query bus.
- `(*Bus).Use(middlewares ...Middleware)`: Registers interceptors in the bus.
- `NewContext(parent context.Context, queryID flux.Identifier, actor flux.Actor, correlationID flux.Identifier, causationID flux.Identifier) Context`: Creates a query context.
- `Register[Q any, R any](bus *Bus, handler func(ctx Context, query Q) (R, error))`: Registers a closure handler.
- `RegisterHandler[Q any, R any](bus *Bus, handler Handler[Q, R])`: Registers an interface handler.
- `Execute[Q any, R any](ctx Context, bus *Bus, query Q) (R, error)`: Executes query `Q` and returns read model `R`.

---

### Package: `github.com/wotek/flux/event`

Provides event distribution to multiple subscribers and access to envelope metadata.

#### Structs & Types

- `Bus`: Registry supporting multiple subscriber handlers per domain event name.

#### Interfaces

- `Context`: Extends `flux.Context` with `EventIdentifier()`, `Stream()`, `Revision()`, `Position()`, and `Metadata()`.
- `Handler[E flux.Event]`: Defines `Handle(ctx Context, event E) error`.

#### Functions

- `New() *Bus`: Creates an event bus.
- `(*Bus).Use(middlewares ...Middleware)`: Registers interceptors in the bus.
- `NewContext(parent context.Context, env flux.Envelope) Context`: Creates an event context from an envelope.
- `Register[E flux.Event](bus *Bus, handler func(ctx Context, event E) error)`: Subscribes a closure handler.
- `RegisterHandler[E flux.Event](bus *Bus, handler Handler[E])`: Subscribes an interface handler.
- `PublishEnvelope(ctx Context, bus *Bus, env flux.Envelope) error`: Dispatches an envelope to all matching subscribers.
- `Publish[E flux.Event](ctx Context, bus *Bus, event E) error`: Wraps and publishes a bare event.

---

### Package: `github.com/wotek/flux/projection`

Engine for maintaining asynchronous read models and tracking global event stream checkpoints.

#### Structs & Types

- `Projector`: Worker that continuously tails an `EventStore` from the last saved position and executes registered event projection handlers.

#### Interfaces

- `Store`: Persistence contract defining `GetPosition(ctx, id) (uint64, error)` and `Update(ctx, id, env, mutate func(txCtx) error) error`.
- `Context`: Extends `event.Context` inside a transactional projection update boundary.

#### Functions

- `New(id flux.Identifier, eventStore flux.EventStore, projStore Store) *Projector`: Creates a Projector.
- `NewContext(parent event.Context) Context`: Creates a projection context.
- `RegisterHandler[E flux.Event](p *Projector, handler func(ctx Context, event E) error)`: Maps a domain event to projection logic.
- `(p *Projector) Start(ctx context.Context) error`: Runs the continuous tailing loop until context cancellation.

---

### Package: `github.com/wotek/flux/workflow`

Orchestration engine coordinating long-running business processes and durable Outbox command dispatching.

#### Structs & Types

- `Orchestrator`: Worker routing global events to specific workflow instances by correlation ID.

#### Interfaces

- `Workflow[W Workflow[W]]`: Go 1.26 self-referencing generic constraint requiring `Identifier() flux.Identifier`, `New() W`, and `Clone() W`.
- `Store[W Workflow[W]]`: Persistence contract for loading workflow state and atomically saving state alongside outbox commands.
- `CheckpointStore`: Interface for persisting and retrieving orchestrator stream checkpoints (`GetPosition`, `SetPosition`).
- `Context`: Extends `event.Context` with `QueuedCommands() []any`.

#### Functions

- `NewOrchestrator(id flux.Identifier, eventStore flux.EventStore, checkpoint CheckpointStore) *Orchestrator`: Creates a Workflow Orchestrator with durable checkpointing.
- `NewContext(parent event.Context) Context`: Creates a workflow context.
- `EnqueueCommand[C any](ctx Context, cmd C)`: Safely enqueues a strongly-typed command into the workflow outbox.
- `RegisterHandler[W Workflow[W], E flux.Event](o *Orchestrator, store Store[W], handler func(ctx Context, workflow W, event E) error)`: Links an event to a workflow step.
- `(o *Orchestrator) Start(ctx context.Context) error`: Runs the orchestrator polling loop.

---

### Store Subpackages (`*/store`)

Subpackages providing concrete storage implementations:

- **`github.com/wotek/flux/event/store`**:
  - `EventStore`: Implementation of `flux.EventStore` with optimistic concurrency validation.
  - `New()`: Constructor.
- **`github.com/wotek/flux/projection/store`**:
  - `ProjectionStore`: Implementation of `projection.Store`.
  - `New()`: Constructor.
- **`github.com/wotek/flux/workflow/store`**:
  - `WorkflowStore[W workflow.Workflow[W]]`: Implementation of `workflow.Store` with an Outbox Relay worker (`StartRelay(ctx)`).
  - `New[W](cmdBus *command.Bus)`: Constructor.
