# Todo App Implementation Guide: `flux` vs `modernice/goes`

This guide analyzes the Todo App example from [`modernice/goes`](https://github.com/modernice/goes/tree/main/examples/todo) and provides a concrete implementation plan for building the same application using our strictly-typed `flux` framework.

## 1. Architectural Comparison

### `modernice/goes` Approach
*   **Dynamic Typing:** Heavily relies on strings for command/event names (`"todo.list.add_task"`) and uses `any` (or basic types like `string` and `[]string`) for payloads.
*   **Reflection-based Routing:** Aggregates and Projections wire their handlers using reflection at runtime (e.g., `event.ApplyWith(list, list.add, TaskAdded)`).
*   **Command Handling in Aggregate:** The Aggregate embeds a `handler.BaseHandler` and processes commands directly.

### Our `flux` Approach
*   **Strict Typing:** Commands and Events are concrete Go structs. Type safety is guaranteed at compile-time via Go 1.18+ Generics.
*   **Reflection-Free Execution:** Uses type-erased closure wrappers for native `O(1)` routing. Event application inside Aggregates uses standard Go type switches instead of reflection.
*   **Separation of Concerns:** Command routing is handled by the `command.Bus`, loading/saving is handled by the `AggregateRepository`, and the Aggregate itself remains a pure domain object containing only business logic.

---

## 2. Implementation Plan

### Step 1: Define Events and Commands
Unlike `goes`, where payloads are untyped `string` or `[]string`, we define explicit structs for every action.

```go
package todo

import "github.com/wotek/flux"

// --- Events ---
type TodoEvent interface {
	flux.Event
	isTodoEvent()
}

type TaskAdded struct { Task string }
func (e TaskAdded) Name() string { return "TaskAdded" }
func (e TaskAdded) isTodoEvent() {}

type TaskRemoved struct { Task string }
func (e TaskRemoved) Name() string { return "TaskRemoved" }
func (e TaskRemoved) isTodoEvent() {}

type TasksDone struct { Tasks []string }
func (e TasksDone) Name() string { return "TasksDone" }
func (e TasksDone) isTodoEvent() {}

// --- Commands ---
type AddTask struct {
	ListID flux.Identifier
	Task   string
}

type RemoveTask struct {
	ListID flux.Identifier
	Task   string
}

type DoneTasks struct {
	ListID flux.Identifier
	Tasks  []string
}
```

### Step 2: Implement the Aggregate
The `TodoListAggregate` aggregate is a pure domain object. It does not know about the `CommandBus` or database.

```go
package todo

import (
	"slices"
	"github.com/wotek/flux"
)

type TodoListAggregate struct {
	flux.AggregateRoot[TodoEvent]
	
	active   []string
	archived []string
}

// New implements the self-referencing generic constraint for instantiation.
func (l *TodoListAggregate) New(stream flux.Stream) *TodoListAggregate {
	list := &TodoListAggregate{}
	list.AggregateRoot = flux.NewAggregateRoot[TodoEvent](stream, flux.NewChangeset[TodoEvent](), list.apply)
	return list
}

// apply mutates state based on historical or new events via a safe type-switch.
func (l *TodoListAggregate) apply(event TodoEvent) error {
	switch e := event.(type) {
	case TaskAdded:
		l.active = append(l.active, e.Task)
	case TaskRemoved:
		l.active = slices.DeleteFunc(l.active, func(t string) bool { return t == e.Task })
	case TasksDone:
		for _, task := range e.Tasks {
			l.active = slices.DeleteFunc(l.active, func(t string) bool { return t == task })
			l.archived = append(l.archived, task)
		}
	}
	return nil
}

// --- Domain Behaviors ---

func (l *TodoListAggregate) Add(task string) {
	if slices.Contains(l.active, task) {
		return // Idempotent
	}
	
	event := TaskAdded{Task: task}
	l.Changeset().Record(event)
	l.apply(event)
}

func (l *TodoListAggregate) Remove(task string) {
	if !slices.Contains(l.active, task) {
		return
	}
	
	event := TaskRemoved{Task: task}
	l.Changeset().Record(event)
	l.apply(event)
}

func (l *TodoListAggregate) Done(tasks ...string) {
	// Filter tasks actually in the list...
	event := TasksDone{Tasks: tasks}
	l.Changeset().Record(event)
	l.apply(event)
}
```

### Step 3: Wire Command Handlers
Commands are processed by external handlers which load the Aggregate, call its methods, and save it.

```go
package todo

import (
	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
)

func RegisterHandlers(bus *command.Bus, repo *flux.AggregateRepository[*TodoListAggregate, TodoEvent]) {
	command.Register(bus, func(ctx command.Context, cmd AddTask) error {
		stream := flux.Stream{Identifier: cmd.ListID}
		list, err := repo.Load(ctx, stream)
		if err != nil { return err }
		
		list.Add(cmd.Task)
		return repo.Save(ctx, list)
	})

	command.Register(bus, func(ctx command.Context, cmd DoneTasks) error {
		stream := flux.Stream{Identifier: cmd.ListID}
		list, err := repo.Load(ctx, stream)
		if err != nil { return err }
		
		list.Done(cmd.Tasks...)
		return repo.Save(ctx, list)
	})
}
```

### Step 4: Create the Read Model (Projector)
To build global statistics (the `Counter` in `goes`), we configure a `flux.Projector` to listen to the Event Store stream. 

```go
package todo

import (
	"github.com/wotek/flux"
	"github.com/wotek/flux/projection"
)

// CounterStore manages the SQL/Redis transactions for the read model.
type CounterStore interface {
	IncrementActive(ctx projection.Context, count int) error
	IncrementArchived(ctx projection.Context, count int) error
	IncrementRemoved(ctx projection.Context, count int) error
}

func StartCounterProjector(id flux.Identifier, eventStore flux.EventStore, projStore projection.Store, statsStore CounterStore) *projection.Projector {
	projector := projection.New(id, eventStore, projStore)

	projection.RegisterHandler(projector, func(ctx projection.Context, e TaskAdded) error {
		return statsStore.IncrementActive(ctx, 1)
	})

	projection.RegisterHandler(projector, func(ctx projection.Context, e TaskRemoved) error {
		statsStore.IncrementRemoved(ctx, 1)
		return statsStore.IncrementActive(ctx, -1)
	})

	projection.RegisterHandler(projector, func(ctx projection.Context, e TasksDone) error {
		statsStore.IncrementArchived(ctx, len(e.Tasks))
		return statsStore.IncrementActive(ctx, -len(e.Tasks))
	})

	return projector
}
```

### Step 5: Wiring the Server
The Server initializes the framework infrastructure (EventStore, CommandBus, Repositories) and runs the background Projectors.

```go
package main

import (
	"context"
	"log/slog"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/store/inmemory"
	"todo"
)

func main() {
	ctx := context.Background()

	// 1. Initialize Infrastructure
	eventStore := inmemory.NewEventStore()
	projStore := inmemory.NewProjectionStore()
	cmdBus := command.NewBus()

	// 2. Initialize Repositories
	repo := flux.NewAggregateRepository[*todo.TodoListAggregate, todo.TodoEvent](eventStore)

	// 3. Register Command Handlers
	todo.RegisterHandlers(cmdBus, repo)

	// 4. Start Read Model Projectors
	projID := flux.NewIdentifierFromString("urn:todo:prod:projections:1:counter:main")
	
	// Assume MemoryCounterStore implements the CounterStore interface from Step 4
	statsStore := &MemoryCounterStore{} 
	projector := todo.StartCounterProjector(projID, eventStore, projStore, statsStore)

	slog.Info("Starting Todo Server...")
	
	// Projectors run in the background, continuously tailing the EventStore
	go projector.Start(ctx)

	// Block main thread (simulating a running server)
	select {}
}
```

### Step 6: Wiring the Client
The Client constructs strongly-typed Commands and routes them through the `command.Bus`. In a real microservices architecture, the client would send an HTTP/gRPC request, and the API gateway would invoke the `CommandBus`.

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/command"
	"todo"
)

func runClient(bus *command.Bus) {
	ctx := context.Background()
	
	// Target a specific TodoListAggregate Aggregate Stream
	listID := flux.NewIdentifierFromString("urn:todo:prod:lists:1:list:abc-123")

	// Helper to generate a dummy command context
	cmdCtx := command.NewContext(ctx, flux.NewIdentifierFromString("urn:todo:prod:commands:1:cmd:uuid"), flux.Actor{}, flux.Identifier{}, flux.Identifier{})

	slog.Info("Adding 10 tasks...")
	for i := 0; i < 10; i++ {
		cmd := todo.AddTask{
			ListID: listID,
			Task:   fmt.Sprintf("Task %d", i+1),
		}
		if err := command.Execute(cmdCtx, bus, cmd); err != nil {
			slog.Error("Failed to add task", "error", err)
			os.Exit(1)
		}
		time.Sleep(100 * time.Millisecond)
	}

	slog.Info("Removing every second task...")
	for i := 0; i < 10; i += 2 {
		cmd := todo.RemoveTask{
			ListID: listID,
			Task:   fmt.Sprintf("Task %d", i+1),
		}
		command.Execute(cmdCtx, bus, cmd)
		time.Sleep(100 * time.Millisecond)
	}

	slog.Info("Marking Task 6 and Task 10 as Done...")
	doneCmd := todo.DoneTasks{
		ListID: listID,
		Tasks:  []string{"Task 6", "Task 10"},
	}
	
	if err := command.Execute(cmdCtx, bus, doneCmd); err != nil {
		slog.Error("Failed to mark tasks done", "error", err)
		os.Exit(1)
	}
	
	slog.Info("Client workflow completed successfully.")
}
```

## Conclusion
By shifting away from `modernice/goes` towards `flux`, the Todo App gains **absolute compile-time safety**. There is no longer a risk of dispatching a command payload with the wrong string identifier, or panicking at runtime because reflection failed to map an event handler. The codebase becomes predictable, deeply integrated with the Go 1.18+ generic type system, and operates blazingly fast without reflection overhead.
