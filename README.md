# flux

[![Go Reference](https://pkg.go.dev/badge/github.com/wotek/flux.svg)](https://pkg.go.dev/github.com/wotek/flux)
[![Go Report Card](https://goreportcard.com/badge/github.com/wotek/flux)](https://goreportcard.com/report/github.com/wotek/flux)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![CI](https://github.com/wotek/flux/actions/workflows/ci.yml/badge.svg)](https://github.com/wotek/flux/actions/workflows/ci.yml)

A lightweight, high-performance, and type-safe **Event Sourcing & CQRS** framework for Go.

`flux` leverages modern Go generics to deliver **100% reflection-free** dispatching (`O(1)` routing) with compile-time type safety, self-referencing generic aggregate roots, and optimistic concurrency control.

---

## Features

* **100% Reflection-Free Execution:** Fast, constant-time `O(1)` routing for commands, queries, and events using type-erased closure wrappers instead of runtime reflection.
* **Type-Safe Generic Aggregates:** Generic aggregate root (`AggregateRoot[TEvent]`) enforcing compile-time event typing and self-referencing aggregate instantiation.
* **Aggregate Snapshotting:** Memento-pattern based snapshots (`SnapshotRepository`) to accelerate loading long-lived aggregates without polluting domain logic with persistence concerns.
* **Sentinel Errors:** Programmatic error evaluation (e.g., `flux.ErrConcurrency`, `flux.ErrAggregateNotFound`) using standard Go `errors.Is()`.
* **Structured Resource Identifiers:** RFC-like URN identifiers (`urn:<org>:<env>:<service>:<account>:<type>:<id>[@version]`) stored as compact, zero-allocation strings.
* **Complete CQRS Ecosystem:**
  * **Command Bus (`command`):** In-memory single-handler routing with `command.Context`.
  * **Query Bus (`query`):** In-memory strongly typed queries returning typed results.
  * **Event Bus (`event`):** Multi-subscriber event routing with `event.Context`.
  * **Projector (`projection`):** Read-model state lifecycle and checkpoint management.
  * **Saga & Orchestrator (`saga`):** Multi-step process coordinators with durable Outbox command dispatching.
* **Context & Metadata Propagation:** First-class auditability preserving `Actor`, `CorrelationIdentifier`, and `CausationIdentifier` across all operations.
* **Pluggable Storage:** Built-in in-memory stores (`event/store`, `projection/store`, `saga/store`) with clean interfaces for implementing durable event and projection databases.

---

## Installation

Requires Go 1.26 or later:

```bash
go get github.com/wotek/flux
```

---

## Quick Start

### 1. Define Domain Events & Aggregate

```go
package main

import (
	"context"
	"fmt"

	"github.com/wotek/flux"
	eventstore "github.com/wotek/flux/event/store"
)

// Domain Event
type AccountCreated struct {
	Owner string
}

func (e AccountCreated) Name() string { return "AccountCreated" }

type MoneyDeposited struct {
	Amount int
}

func (e MoneyDeposited) Name() string { return "MoneyDeposited" }

// Aggregate
type BankAccount struct {
	flux.AggregateRoot[flux.Event]
	Owner   string
	Balance int
}

func (a *BankAccount) New(stream flux.Stream) *BankAccount {
	return NewBankAccount(stream)
}

func NewBankAccount(stream flux.Stream) *BankAccount {
	a := &BankAccount{}
	a.AggregateRoot = flux.NewAggregateRoot[flux.Event](stream, flux.NewChangeset[flux.Event](), a.apply)
	return a
}

func (a *BankAccount) apply(event flux.Event) error {
	switch e := event.(type) {
	case AccountCreated:
		a.Owner = e.Owner
	case MoneyDeposited:
		a.Balance += e.Amount
	}
	return nil
}

func (a *BankAccount) Create(owner string) {
	a.Changeset().Record(AccountCreated{Owner: owner})
}

func (a *BankAccount) Deposit(amount int) {
	a.Changeset().Record(MoneyDeposited{Amount: amount})
}
```

### 2. Save and Rehydrate with Repository

```go
func main() {
	ctx := context.Background()

	// In-memory event store
	eventStore := eventstore.New()
	repo := flux.NewAggregateRepository[*BankAccount, flux.Event](eventStore)

	// Create stream identifier
	id := flux.MustParseIdentifier("urn:bank:prod:core:acc123:account:main")
	stream := flux.Stream{Identifier: id}

	// Create new account
	account := NewBankAccount(stream)
	account.Create("Alice")
	account.Deposit(250)

	// Persist changes with optimistic concurrency
	if err := repo.Save(ctx, account); err != nil {
		panic(err)
	}

	// Rehydrate from event stream
	loaded, err := repo.Load(ctx, stream)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Loaded Account: %s, Balance: $%d\n", loaded.Owner, loaded.Balance)
	// Output: Loaded Account: Alice, Balance: $250
}
```

### 3. Route Commands & Events

```go
import (
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/event"
)

// Register a command handler
cmdBus := command.New()
command.Register(cmdBus, func(ctx command.Context, cmd CreateAccountCommand) error {
    // Command execution logic
    return nil
})

// Dispatch a command
cmdCtx := command.NewContext(context.Background(), cmdID, actor, correlationID, causationID)
if err := command.Execute(cmdCtx, cmdBus, CreateAccountCommand{Owner: "Alice"}); err != nil {
    log.Fatal(err)
}
```

---

## Architecture & Documentation

* **[Architecture & Dependency Graph](docs/ARCHITECTURE.md):** Detailed module hierarchy, cycle analysis, and complete exported API reference.
* **[API Design Specification](docs/API.md):** Complete specification of aggregates, repositories, contexts, projections, and sagas.
* **[Todo Reference Application](example/todo/README.md):** Complete CQRS and Event Sourced reference implementation with domain events, co-located handlers, and read-model projections.
* **[Identifier](docs/API.md#identifier):** Uniform Resource Name (`URN`) addressing any system component.
* **[Envelope](docs/API.md#event-and-envelope):** Wraps domain events with revision, global position, actor, timestamps, and causation data.
* **[AggregateRepository](docs/API.md#aggregate-repository):** Handles snapshotting, rehydration, and atomic event appending.
* **[Projector](docs/API.md#projections-package-projection):** Continuous global stream tailing and read-model checkpointing.
* **[Orchestrator / Saga](docs/API.md#sagas--process-managers-package-saga):** Coordinates multi-step workflows with durable outbox delivery.

---

## Testing

Run unit tests and race detection:

```bash
go test -v -race ./...
```

---

## License

Distributed under the [MIT License](LICENSE).
