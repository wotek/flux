# Aggregates & Changesets

Aggregates are the absolute heart of the write model in any Event-Sourced system. They act as consistency boundaries—their sole purpose is to enforce business rules (invariants) and ensure that no invalid actions occur.

If an action is valid, the Aggregate emits an **Event** (a fact) indicating what happened.

## Reflection-Free Design

Most Go event-sourcing libraries rely on `reflect` to magically bind events to aggregate state. `flux` takes a completely different approach, leveraging **Go 1.27+ Generics** to provide a blazing-fast, type-safe, and explicitly defined Aggregate lifecycle.

To create an Aggregate in `flux`, you embed `flux.AggregateRoot[E flux.Event]` into your struct.

```go
import "github.com/wotek/flux"

type BankAccount struct {
	// 1. Embed the generic AggregateRoot
	flux.AggregateRoot[flux.Event]
	
	// 2. Define the Aggregate's internal state
	Owner   string
	Balance int
	Closed  bool
}
```

By embedding the root, your struct automatically gains the ability to track uncommitted events, manage sequence versions, and hold a reference to its native `Stream`.

## The Aggregate Lifecycle

The lifecycle of an Aggregate in `flux` is highly structured.

### 1. The Factory (Instantiation)

When the `AggregateRepository` loads an aggregate from the database, it needs a way to instantiate a clean, empty version of your struct. It does this by calling the required `New()` method.

You must provide a `New()` method and a domain-level constructor that binds the `flux.Changeset` and the `apply` state mutator.

```go
// New is required by the flux.AggregateRoot interface for generic rehydration.
func (a *BankAccount) New(stream flux.Stream) *BankAccount {
	return NewBankAccount(stream)
}

// NewBankAccount is your domain constructor.
func NewBankAccount(stream flux.Stream) *BankAccount {
	a := &BankAccount{}
	
	// Bind the stream, a new changeset, and the apply function
	a.AggregateRoot = flux.NewAggregateRoot[flux.Event](stream, flux.NewChangeset[flux.Event](), a.apply)
	return a
}
```

### 2. State Mutation (`apply`)

The golden rule of Event Sourcing is that **state is only ever mutated by events**. 

When an event is loaded from the database during rehydration, or when a new event is recorded locally, the framework routes it to your `apply()` method. **This is the ONLY place in your entire codebase where you should modify the Aggregate's fields.**

```go
func (a *BankAccount) apply(event flux.Event) {
	switch e := event.(type) {
	case AccountOpened:
		a.Owner = e.Owner
	case MoneyDeposited:
		a.Balance += e.Amount
	case AccountClosed:
		a.Closed = true
	}
}
```

### 3. Business Logic & Invariants

Public methods on your aggregate are where you evaluate business logic and guard your invariants. 

If a request violates a business rule, return an error. If the request is valid, you mutate state and record the outcome as an Event using the `Changeset`. You NEVER mutate state directly outside of `apply`.

Domain methods are strictly responsible for mutating state by first calling their internal apply logic, and then recording the event to the Changeset. The framework does not provide a public Record helper to prevent encapsulation leaks.

```go
import "errors"

var ErrAccountClosed = errors.New("account is closed")
var ErrNegativeDeposit = errors.New("cannot deposit negative amount")

func (a *BankAccount) Deposit(amount int) error {
	// 1. Enforce Invariants
	if a.Closed {
		return ErrAccountClosed
	}
	if amount <= 0 {
		return ErrNegativeDeposit
	}
	
	// 2. Record the Event
	// Mutate state via apply and record the event to the uncommitted changeset
	event := MoneyDeposited{Amount: amount}
	a.apply(event)
	a.Changeset().Record(event)
	
	return nil
}
```

## The Changeset Mechanism

In `flux`, domain methods are strictly responsible for mutating state by first calling their internal apply logic, and then recording the event to the Changeset. The framework does not provide a public Record helper to prevent encapsulation leaks.

When recording an event:
1. The event is passed to `apply()` to synchronously update the in-memory state of your aggregate.
2. The event is appended to an internal buffer of "Uncommitted Events" inside the `Changeset` via `a.Changeset().Record(event)`.

When you eventually call `repository.Save(ctx, aggregate)`, the repository extracts this buffer of uncommitted events from the `Changeset` and flushes them to the `EventStore`. 

Once the save is successful, the repository clears the `Changeset`.

## Best Practices

### Keep Aggregates Small
Aggregates are transactional boundaries. Every time you save an Aggregate, the `EventStore` enforces optimistic concurrency on the entire stream. If two users try to modify the same Aggregate simultaneously, one will fail. 

Therefore, do not design massive Aggregates (like putting every `Order` inside a single `Customer` aggregate). Separate them into different streams (`urn:customer:123` and `urn:order:456`) to minimize contention.

### Don't Project Inside Aggregates
Your Aggregate state should only contain the bare minimum data required to enforce business invariants. Do not store data in your Aggregate just because the UI needs to display it later. Use **Projections** to build Read Models for the UI.

### Infallible `apply()`
Notice that `apply(event flux.Event)` does not return an error. By the time an event reaches `apply()`, it represents an immutable historical fact. Rejecting a fact during hydration breaks the system and prevents an aggregate from loading. All validation errors strictly belong in domain methods prior to invoking `apply` and recording to the `Changeset`.
