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
│   ├── create_list.go                   # CreateList command + CreateListHandler
│   ├── add_task.go                      # AddTask command + AddTaskHandler
│   ├── remove_task.go                   # RemoveTask command + RemoveTaskHandler
│   ├── done_tasks.go                    # DoneTasks command + DoneTasksHandler
│   └── register.go                      # RegisterHandlers() batch registration helper
│
├── queries/                             # Queries and co-located query handlers
│   ├── doc.go                           # Package documentation
│   ├── get_counter.go                   # GetCounter query + GetCounterHandler
│   ├── get_todo_list.go                 # GetTodoList query + GetTodoListHandler
│   ├── get_lists.go                     # GetLists query + GetListsHandler
│   └── register.go                      # RegisterHandlers() batch registration helper
│
├── events/                              # Strongly-typed domain events
│   ├── doc.go                           # Package documentation
│   ├── event.go                         # TodoEvent sealed interface
│   ├── list_created.go                  # ListCreated event struct
│   ├── task_added.go                    # TaskAdded event struct
│   ├── task_removed.go                  # TaskRemoved event struct
│   └── tasks_done.go                    # TasksDone event struct
│
├── projections/                         # Read-model projections
│   ├── doc.go                           # Projections package documentation
│   ├── counter/                         # Dedicated counter projection subpackage
│   │   ├── doc.go                       # Counter package documentation
│   │   ├── counter.go                   # Counter read model struct (Active, Archived, Removed)
│   │   ├── store.go                     # Store persistence interface
│   │   ├── memory_store.go              # MemoryStore thread-safe implementation
│   │   ├── projector.go                 # NewProjector constructor and handlers
│   │   └── projector_test.go            # Unit test for counter projector
│   └── lists/                           # Dedicated lists projection subpackage
│       ├── doc.go                       # Lists package documentation
│       ├── list_summary.go              # ListSummary read model struct
│       ├── store.go                     # Store persistence interface
│       ├── memory_store.go              # MemoryStore thread-safe implementation
│       ├── projector.go                 # NewProjector constructor and handlers
│       └── projector_test.go            # Unit test for lists projector
│
├── server/                              # Server orchestration & HTTP gateway
│   ├── doc.go                           # Package documentation
│   ├── server.go                        # Server struct (buses, stores, projector, HTTP lifecycle)
│   ├── options.go                       # Functional options (WithHTTP, WithEventStore, etc.)
│   ├── http_handler.go                  # HTTP gateway endpoints (/tasks, /tasks/done, /counter)
│   └── server_test.go                   # Server & HTTP integration tests
│
├── client/                              # Client abstractions & implementations
│   ├── doc.go                           # Package documentation
│   ├── client.go                        # Client interface definition
│   ├── in_memory_client.go              # InMemoryClient direct bus implementation
│   ├── http_client.go                   # HTTPClient JSON REST implementation
│   ├── interactive.go                   # Interactive terminal CLI dashboard & command loop
│   ├── workflow.go                      # Canonical 10-task demo workflow runner
│   └── client_test.go                   # Tests for InMemoryClient, HTTPClient & interactive mode
│
└── cmd/                                 # Application entry points
    ├── todo/                            # All-in-one runner (in-process server + client simulation)
    │   ├── doc.go
    │   ├── main.go
    │   └── main_test.go
    ├── server/                          # Standalone HTTP daemon entry point
    │   ├── doc.go
    │   ├── main.go
    │   └── main_test.go
    └── client/                          # Standalone CLI HTTP client entry point
        ├── doc.go
        ├── main.go
        └── main_test.go
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
5. **Decoupled Server & Client (`server/` & `client/`)**:
   - `server.Server` manages stores, buses, background projector workers, and optional HTTP gateway.
   - `client.Client` interface provides polymorphism across `InMemoryClient` (in-process bus dispatch) and `HTTPClient` (remote HTTP gateway).
6. **No Namespace Collisions**:
   - Domain subpackages use plural names (`commands`, `events`, `queries`, `projections`), cleanly preventing collisions with framework packages (`flux/command`, `flux/event`, `flux/query`, `flux/projection`).

---

## Prerequisites

* **Go**: Modern Go (Go 1.26 or later recommended)

---

## How to Compile & Run

### Option 1: All-In-One Runner (`cmd/todo`)

Showcases how everything works together in one executable:

```bash
cd example/todo
go run ./cmd/todo
```

#### Expected Output

```text
time=2026-09-16T09:22:14.305+02:00 level=INFO msg="initializing infrastructure..."
time=2026-09-16T09:22:14.305+02:00 level=INFO msg="client: adding 10 tasks..."
time=2026-09-16T09:22:14.305+02:00 level=INFO msg="starting background counter projector..."
time=2026-09-16T09:22:14.306+02:00 level=INFO msg="client: removing odd tasks (1, 3, 5, 7, 9)..."
time=2026-09-16T09:22:14.306+02:00 level=INFO msg="client: marking Task 6 and Task 10 as completed..."
time=2026-09-16T09:22:14.408+02:00 level=INFO msg="client: read model verified successfully" active=3 archived=2 removed=5
```

---

### Option 2: Standalone Server + Standalone Client

Run the server daemon in one terminal and the client in another.

#### 1. Start Server Daemon

```bash
cd example/todo
go run ./cmd/server -addr :8080
```

Output:
```text
level=INFO msg="starting todo cqrs server" addr=:8080
level=INFO msg="server: starting background counter projector..."
level=INFO msg="server: starting HTTP gateway..." addr=:8080
```

#### 2. Run Interactive Client (Default)

In a separate terminal:

```bash
cd example/todo
go run ./cmd/client -server http://localhost:8080
```

This starts the interactive terminal dashboard:

```text
================================================================================
  FLUX CQRS TODO APP — Personal Projects
  URN: urn:todo:prod:lists:1:list:personal
  Global Stats: Active: 2 | Archived: 1 | Removed: 1 | Total Lists: 2
================================================================================

ACTIVE TASKS (2):
  ▶ [1] Buy groceries  <-- [SELECTED]
    [2] Read Flux documentation

ARCHIVED / COMPLETED (1):
    ✓ Set up project

Commands:
  [n] Next item      [p] Prev item      [a] Add task       [d] Delete selected
  [c] Mark done      [nl] New list      [l] Switch list    [tab] Next list
  [ls] Overview      [r] Refresh        [q] Quit
  (Or type: add <text> | del <num> | done <num> | <num> to select)
--------------------------------------------------------------------------------
todo> 
```

**Interactive Controls:**
* `n` or `<Enter>`: Cycle cursor to next task in current list
* `p`: Cycle cursor to previous task in current list
* `a` or `add <text>`: Add a new task to current list
* `d` or `del [num]`: Delete currently selected task (or task by number)
* `c` or `done [num]`: Mark currently selected task as completed
* `1`, `2`, ...: Select task by number directly
* `nl` or `new <title>`: Create a new Todo List aggregate and switch to it
* `ls` or `lists`: View read-model overview of all Todo Lists with summary counts
* `l` or `switch [num|urn]`: Switch between Todo Lists
* `tab` or `nextlist`: Cycle directly to the next Todo List
* `r`: Refresh projection stats and task list
* `q`: Exit

#### 3. Run Automated Workflow Demo

To run the automated 10-task test workflow without interactive prompts:

```bash
go run ./cmd/client -server http://localhost:8080 -demo
```

---

### Compile into Standalone Binaries

```bash
cd example/todo
mkdir -p bin
go build -o bin/todo ./cmd/todo
go build -o bin/server ./cmd/server
go build -o bin/client ./cmd/client
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
