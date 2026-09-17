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
4. **Dedicated Projection Subpackages (`projections/counter/` & `projections/lists/`)**:
   - Projections are isolated in their own subpackages containing the read-model struct, storage contract, thread-safe implementation, and event projector worker.
5. **Multiple Domain Aggregates & Lists Catalog**:
   - The system supports arbitrary concurrent `TodoListAggregate` instances (e.g. Work, Personal).
   - A dedicated `lists` read-model projector indexes all aggregates into an overview catalog with real-time active and archived task counts.
6. **Decoupled Server & Client (`server/` & `client/`)**:
   - `server.Server` manages stores, buses, background projector workers, and optional HTTP gateway.
   - `client.Client` interface provides polymorphism across `InMemoryClient` (in-process bus dispatch) and `HTTPClient` (remote HTTP gateway).
7. **No Namespace Collisions**:
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

The interactive terminal client features a **hierarchical two-screen navigation flow**:

```text
┌────────────────────────────────────────────────────────┐
│            Screen 1: Lists Catalog (Entry)             │
│  - Shows all available Todo Lists & summary stats      │
│  - Cycle through lists, create new lists, or open one  │
└───────────────────────┬────────────────────────────────┘
                        │
                        │ [Enter / o / <number>]  (Open selected list)
                        │ [b / back]             (Return to catalog)
                        ▼
┌────────────────────────────────────────────────────────┐
│             Screen 2: Task Detail Screen               │
│  - Shows current list title, URN, active & completed   │
│  - Cycle tasks, add, remove, and archive/complete      │
└────────────────────────────────────────────────────────┘
```

---

##### Screen 1: Available Lists (Entry Screen)

When launched, the client displays the catalog of all known Todo List aggregates powered by the `lists` read-model projection:

```text
================================================================================
  FLUX CQRS TODO APP — All Todo Lists
  Global Stats: Active: 5 | Archived: 2 | Removed: 1 | Total Lists: 2
================================================================================

AVAILABLE TODO LISTS (2):
  ▶ [1] Work Tasks (Active: 3, Archived: 1)  <-- [SELECTED]
        URN: urn:todo:prod:lists:1:list:work-1234
    [2] Personal Tasks (Active: 2, Archived: 1)
        URN: urn:todo:prod:lists:1:list:personal-5678

Status: Welcome! Select a list to open, or create a new one.

Commands:
  [n] Next list      [p] Prev list      [o/Enter] Open list
  [a] New list       [r] Refresh        [q] Quit
  (Or type: new <title> | open <num> | <num> to open directly)
--------------------------------------------------------------------------------
lists> 
```

**Lists Screen Controls:**
* `n` / `p`: Cycle selection cursor forward and backward across lists
* `<Enter>` or `o` / `open`: Open the currently selected list into Screen 2
* `1`, `2`, ...: Open a list directly by its number
* `a` or `new <title>`: Create a new Todo List aggregate and open its task screen
* `r`: Refresh lists catalog from the read-model projection
* `q`: Exit

---

##### Screen 2: Task Detail Screen

Opening a list navigates into its tasks screen, displaying active and archived items:

```text
================================================================================
  FLUX CQRS TODO APP — Work Tasks
  URN: urn:todo:prod:lists:1:list:work-1234
  List Status: 3 Active | 1 Archived
================================================================================

ACTIVE TASKS (3):
  ▶ [1] Prepare release notes  <-- [SELECTED]
    [2] Deploy staging cluster
    [3] Update documentation

ARCHIVED / COMPLETED (1):
    ✓ Initial architecture review

Status: Added task: "Update documentation"

Commands:
  [n] Next task      [p] Prev task      [a] Add task       [d] Delete task
  [c] Mark done      [b] Back to lists  [r] Refresh        [q] Quit
  (Or type: add <text> | del <num> | done <num> | <num> to select)
--------------------------------------------------------------------------------
tasks> 
```

**Tasks Screen Controls:**
* `n` or `<Enter>`: Cycle selection cursor forward through active tasks
* `p`: Cycle selection cursor backward through active tasks
* `a` or `add <task>`: Add a new task to this list
* `d` or `del [num]`: Remove the selected task (or by number)
* `c` or `done [num]`: Mark the selected task as completed / archived (or by number)
* `1`, `2`, ...: Select a task directly by number
* `b` or `back`: Return back to Screen 1 (Lists Catalog)
* `r`: Refresh tasks from the aggregate
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
