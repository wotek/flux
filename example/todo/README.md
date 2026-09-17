# Todo Reference Application

A reference CQRS and Event-Sourced Todo List application built using the [`flux`](https://github.com/wotek/flux) framework.

This application illustrates how to architect a real-world Go application using strict compile-time type safety, reflection-free `O(1)` routing, and vertical slice co-location without dynamic runtime reflection.

---

## Project Layout

```text
example/todo/
├── go.mod                               # Standalone module (github.com/wotek/flux/example/todo)
├── doc.go                               # Package documentation
├── aggregate.go                         # Pure domain aggregate (TodoListAggregate)
├── aggregate_test.go                    # Unit tests for aggregate logic
├── integration_test.go                  # End-to-end CQRS workflow integration test
├── README.md                            # This guide
│
├── commands/                            # Commands and co-located command handlers
│   ├── doc.go                           # Package documentation
│   ├── add_task.go                      # AddTask command + AddTaskHandler
│   ├── remove_task.go                   # RemoveTask command + RemoveTaskHandler
│   ├── done_tasks.go                    # DoneTasks command + DoneTasksHandler
│   └── register.go                      # RegisterHandlers() batch registration helper
│
├── queries/                             # Queries and co-located query handlers
│   ├── doc.go                           # Package documentation
│   ├── get_counter.go                   # GetCounter query + GetCounterHandler
│   └── register.go                      # RegisterHandlers() batch registration helper
│
├── events/                              # Strongly-typed domain events
│   ├── doc.go                           # Package documentation
│   ├── event.go                         # TodoEvent sealed interface
│   ├── task_added.go                    # TaskAdded event struct
│   ├── task_removed.go                  # TaskRemoved event struct
│   └── tasks_done.go                    # TasksDone event struct
│
├── projections/                         # Read-model projections
│   ├── doc.go                           # Projections package documentation
│   └── counter/                         # Dedicated counter projection subpackage
│       ├── doc.go                       # Counter package documentation
│       ├── counter.go                   # Counter read model struct (Active, Archived, Removed)
│       ├── store.go                     # Store persistence interface
│       ├── memory_store.go              # MemoryStore thread-safe implementation
│       ├── projector.go                 # NewProjector constructor and handlers
│       └── projector_test.go            # Unit test for counter projector
│
└── cmd/todo/                            # Application entry point
    ├── doc.go                           # main package documentation
    ├── main.go                          # Executable server & client simulation
    └── main_test.go                     # Runner smoke test
```

---

## Architectural Highlights

1. **Pure Domain Aggregate (`aggregate.go`)**:
   - The aggregate (`TodoListAggregate`) embeds `flux.AggregateRoot[events.TodoEvent]` and contains only domain logic.
   - It has no dependency on the command bus, database drivers, or network transports.
2. **Sealed Domain Events (`events/`)**:
   - Each event (`TaskAdded`, `TaskRemoved`, `TasksDone`) is an explicit, immutable struct in its own file implementing `Name() string` and an unexported `isTodoEvent()` marker method.
3. **Vertical Slice Handlers (`commands/` & `queries/`)**:
   - Each command and its corresponding handler struct live in the same file (e.g. `AddTask` and `AddTaskHandler` in `commands/add_task.go`).
   - Each query and its corresponding handler struct live in the same file (e.g. `GetCounter` and `GetCounterHandler` in `queries/get_counter.go`).
4. **Dedicated Projection Subpackage (`projections/counter/`)**:
   - Projections are isolated in their own subpackages containing the read-model struct (`Counter`), the storage contract (`Store`), the thread-safe implementation (`MemoryStore`), and the event projector (`NewProjector`).
5. **No Namespace Collisions**:
   - Domain subpackages use plural names (`commands`, `events`, `queries`, `projections`), cleanly preventing collisions with framework packages (`flux/command`, `flux/event`, `flux/query`, `flux/projection`).

---

## Prerequisites

* **Go**: Modern Go (Go 1.26 or later recommended)

---

## How to Compile & Run

### 1. Run Directly with `go run`

From the `example/todo` directory:

```bash
cd example/todo
go run ./cmd/todo
```

Or from the repository root:

```bash
go run -C example/todo ./cmd/todo
```

#### Expected Output

```text
time=2026-09-16T09:15:47.305+02:00 level=INFO msg="initializing infrastructure..."
time=2026-09-16T09:15:47.305+02:00 level=INFO msg="client: adding 10 tasks..."
time=2026-09-16T09:15:47.305+02:00 level=INFO msg="starting background counter projector..."
time=2026-09-16T09:15:47.306+02:00 level=INFO msg="client: removing odd tasks (1, 3, 5, 7, 9)..."
time=2026-09-16T09:15:47.306+02:00 level=INFO msg="client: marking Task 6 and Task 10 as completed..."
time=2026-09-16T09:15:47.408+02:00 level=INFO msg="client: read model verified successfully" active=3 archived=2 removed=5
```

### 2. Compile into a Binary

```bash
cd example/todo
mkdir -p bin
go build -o bin/todo ./cmd/todo
./bin/todo
```

---

## How to Test

### Run All Unit & Integration Tests

From the `example/todo` directory:

```bash
cd example/todo
go test -v -count=1 -race ./...
```

### Run Static Analysis & Linter

```bash
cd example/todo
go vet ./...
```
