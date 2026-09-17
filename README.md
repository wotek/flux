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
* **Structured Resource Identifiers:** RFC-like URN identifiers (`urn:<org>:<env>:<service>:<account>:<type>:<id>[@version]`) stored as compact, zero-allocation strings.
* **Complete CQRS Ecosystem:**
  * **Command Bus:** Strict single-handler execution with context propagation.
  * **Query Bus:** Strongly typed queries returning typed results.
  * **Event Bus:** Multi-subscriber asynchronous/synchronous event routing.
  * **Projector:** Projection state lifecycle management with checkpoint tracking.
  * **Saga & Orchestrator:** Multi-step business transaction coordinators with compensation and causal metadata.
* **Context & Metadata Propagation:** First-class auditability preserving `Actor`, `CorrelationIdentifier`, and `CausationIdentifier` across all events and commands.
* **Pluggable Storage:** Built-in in-memory stores (`store/inmemory`) with clean interfaces for implementing durable event and projection databases.

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
	"github.com/wotek/flux/store/inmemory"
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
	eventStore := inmemory.NewEventStore()
	repo := flux.NewAggregateRepository[*BankAccount, flux.Event](eventStore)

	// Create stream identifier
	id := flux.NewIdentifierFromString("urn:bank:prod:core:acc123:account:main")
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
// Register a command handler
cmdBus := flux.NewCommandBus()
flux.RegisterCommand(cmdBus, func(ctx flux.CommandContext, cmd CreateAccountCommand) error {
    // Command execution logic
    return nil
})

// Dispatch a command
cmdCtx := flux.NewCommandContext(context.Background(), actor, correlationID)
if err := cmdBus.Dispatch(cmdCtx, CreateAccountCommand{Owner: "Alice"}); err != nil {
    log.Fatal(err)
}
```

---

## Architecture & Concepts

* **[Stream](docs/API.md#stream):** The append-only sequence of immutable events belonging to an aggregate.
* **[Identifier](docs/API.md#identifier):** Uniform Resource Name (`URN`) addressing any system component.
* **[Envelope](docs/API.md#event-and-envelope):** Wraps domain events with revision, global position, actor, timestamps, and causation data.
* **[AggregateRepository](docs/API.md#aggregate-repository):** Handles snapshotting, rehydration, and atomic event appending.
* **[Orchestrator / Saga](docs/API.md#saga-and-orchestrator):** Manages multi-step processes and saga workflows with step compensations.

For complete specifications and detailed design documentation, see [docs/API.md](docs/API.md).

---

## Testing

Run unit tests and race detection:

```bash
go test -v -race ./...
```

---

## License

Distributed under the [MIT License](LICENSE).
