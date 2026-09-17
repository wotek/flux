# Implementation Roadmap

## Phase 0: Setup
- [x] **Project Scaffolding**: Create the root repository structure and `go.mod` (module `github.com/wotek/flux`).

## Phase 1: Core Primitives
- [x] **Identifier**: Implement the zero-allocation URN-based `Identifier` struct and its lazy-parsing methods.
- [x] **Stream**: Implement the `Stream` struct representing an entity's event log.
- [x] **Actor**: Implement the `Actor` struct for RBAC and identity metadata.
- [x] **Event**: Define the `Event` empty interface.
- [x] **Envelope**: Implement the Envelope struct with Metadata maps.

## Phase 2: Domain Aggregates
- [x] **Changeset**: Implement the `Changeset` interface and a default slice-backed implementation (`NewChangeset`).
- [x] **Aggregate Interface**: Define the Go 1.26 self-referencing `Aggregate` interface.
- [x] **AggregateRoot**: Implement the embeddable `AggregateRoot` struct, handling Revision tracking, Changeset integration, and the `FromEvents` replay logic.

## Phase 3: Persistence Layer
- [x] **StreamIterator**: Define the `iter.Seq2[Envelope, error]` alias (Go 1.23).
- [x] **EventStore**: Define the `EventStore` interface contract (`Append`, `Read`, `Stream`).
- [x] **InMemoryEventStore**: Implement a thread-safe, memory-backed `EventStore` for testing and local development.
- [x] **AggregateRepository**: Implement the concrete `AggregateRepository` struct.
  - [x] Implement `Load` (instantiating aggregates via `New()`).
  - [x] Implement `Save` (extracting Actor/CorrelationID from Context, wrapping envelopes, appending to store).

## Phase 4: Contexts & CQRS Messaging
- [x] **Typed Contexts**: Implement `Context`, `CommandContext`, `QueryContext`, `EventContext`, `ProjectionContext`, and `SagaContext` interfaces, along with their constructors.
- [x] **Handlers**: Define `CommandHandler`, `QueryHandler`, and `EventHandler` interfaces.
- [x] **CommandBus**: Implement `CommandBus` struct, `RegisterCommandHandler`, `ExecuteCommand` (sync), and `ExecuteCommandAsync` (fire-and-forget).
- [x] **QueryBus**: Implement `QueryBus` struct, `RegisterQueryHandler`, and type-safe `ExecuteQuery`.
- [x] **EventBus**: Implement `EventBus` struct, `RegisterEventHandler`, `PublishEvent`, and `PublishEnvelope`.

## Phase 5: Projections (Read Models)
- [x] **ProjectionStore**: Define the transactional `ProjectionStore` interface.
- [x] **InMemoryProjectionStore**: Implement a memory-backed store that simulates transactional bounds for testing.
- [x] **Projector**: Implement the background `Projector` engine.
  - [x] Tailing the EventStore global stream.
  - [x] Wrapping handler execution inside the `ProjectionStore.Update` transaction.
  - [x] `RegisterProjectionHandler` implementation.

## Phase 6: Sagas (Process Managers)
- [x] **Saga Interfaces**: Define the Go 1.26 `Saga` interface and `SagaStore` interface.
- [x] **InMemorySagaStore**: Implement a memory-backed store with a simulated outbox relay process.
- [x] **SagaContext & Outbox**: Implement the unexported `dispatch` logic and the `EnqueueCommand` helper function.
- [x] **Orchestrator**: Implement the `Orchestrator` engine.
  - [x] Loading the saga state.
  - [x] Executing handlers.
  - [x] Transactionally persisting updated state and outbox commands via `SagaStore.Save`.
