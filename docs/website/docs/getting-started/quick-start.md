# Quick Start

In this Quick Start, we'll build a minimal **Todo Application** utilizing the core concepts of `flux`: Domain Events, Aggregates, Repositories, and Command Routing. 

Unlike traditional CRUD applications where you store the *current state* of a Todo List in a database row, Event Sourcing stores every *change* as a discrete event. The current state is then derived by replaying those events.

## 1. Domain Events

Events are the absolute single source of truth in our system. They represent facts—things that have already happened. 

In `flux`, an Event is simply a Go struct that implements the `flux.Event` interface (providing a `Name() string` method).

```go
// TaskAdded is emitted when a new task is appended to the Todo list.
type TaskAdded struct {
	Title string
}

func (e TaskAdded) Name() string { return "TaskAdded" }
```

## 2. The Aggregate

Aggregates represent your Write Model. They enforce business invariants (rules) and emit events when actions are valid.

To define an Aggregate in `flux`, you embed `flux.AggregateRoot[E flux.Event]`. 

```go
type TodoList struct {
	flux.AggregateRoot[flux.Event]
	Tasks []string // The derived state
}
```

### Aggregate Factory & State Mutation
When `flux` loads an aggregate from the database, it needs a way to instantiate it and a way to apply historical events to it. We provide this via a `New()` method and an `apply()` mutator.

```go
// New is required by the framework to instantiate empty instances during rehydration.
func (l *TodoList) New(stream flux.Stream) *TodoList {
	return NewTodoList(stream)
}

// NewTodoList is our domain constructor.
func NewTodoList(stream flux.Stream) *TodoList {
	l := &TodoList{}
	// We bind the stream, the changeset, and the state mutator (apply).
	l.AggregateRoot = flux.NewAggregateRoot[flux.Event](stream, flux.NewChangeset[flux.Event](), l.apply)
	return l
}

// apply is the ONLY place state is ever mutated!
func (l *TodoList) apply(event flux.Event) error {
	switch e := event.(type) {
	case TaskAdded:
		l.Tasks = append(l.Tasks, e.Title)
	}
	return nil
}
```

### Business Logic
Public methods evaluate rules and **Record** events. They never modify state directly.

```go
func (l *TodoList) AddTask(title string) {
    if title == "" {
        return // Business rule: no empty tasks
    }
	// Record the event. The framework will automatically route this to apply()
	l.Changeset().Record(TaskAdded{Title: title})
}
```

## 3. Command Routing & Persistence

Now we wire the system together using the `AggregateRepository` and the `Command Bus`.

```go
func main() {
    ctx := context.Background()

    // 1. Initialize the Event Store & Repository
    eventStore := eventstore.New() // In-Memory backend for testing
    repo := flux.NewAggregateRepository[*TodoList, flux.Event](eventStore)

    // 2. Initialize the Command Bus
    cmdBus := command.New()

    // 3. Register a Command Handler
    command.Register(cmdBus, func(ctx command.Context, cmd AddTaskCommand) error {
        stream := flux.Stream{Identifier: flux.MustParseIdentifier("urn:todo:list:1")}
        
        // Load the aggregate from the Event Store
        list, err := repo.Load(ctx, stream)
        if err != nil {
            // If it doesn't exist yet, instantiate a new one
            list = NewTodoList(stream)
        }

        // Execute domain logic
        list.AddTask(cmd.Title)

        // Save the changes back to the Event Store (Optimistic Concurrency guaranteed)
        return repo.Save(ctx, list)
    })

    // 4. Dispatch the Command
    cmdCtx := command.NewContext(ctx, flux.MustParseIdentifier("urn:cmd:1"), flux.Actor{}, flux.Identifier{})
    command.Execute(cmdCtx, cmdBus, AddTaskCommand{Title: "Buy milk"})
}
```

## Summary

In just a few lines of code, we built a fully event-sourced application. 
- We strictly decoupled our **Intents** (`AddTaskCommand`) from our **Facts** (`TaskAdded`).
- State mutations are cleanly isolated in `apply()`.
- Optimistic concurrency and database transactions are handled entirely by the `flux.AggregateRepository`.
