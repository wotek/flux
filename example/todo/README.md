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
│   ├── options.go                       # Functional options (WithHTTP, WithLogger, WithEventStore)
│   ├── http_handler.go                  # HTTP gateway endpoints (REST + GET /events SSE)
│   ├── response_recorder.go             # HTTP status recorder for logging middleware
│   ├── sse_event_payload.go             # SSE JSON event payload definition
│   └── server_test.go                   # Server & HTTP integration tests
│
├── client/                              # Client abstractions & implementations
│   ├── doc.go                           # Package documentation
│   ├── client.go                        # Client interface definition
│   ├── event_notification.go            # Live SSE event notification model
│   ├── in_memory_client.go              # InMemoryClient direct bus implementation
│   ├── http_client.go                   # HTTPClient JSON REST & SSE stream implementation
│   ├── interactive.go                   # Interactive terminal TUI runner (Bubble Tea)
│   ├── tui_model.go                     # Bubble Tea Elm-architecture model (Init, Update, View)
│   ├── tui_items.go                     # List & Task items adapting to bubbles/list.Item
│   ├── tui_keys.go                      # Custom keybindings & help definitions
│   ├── tui_styles.go                    # Lip Gloss box, badge, and color styles
│   ├── workflow.go                      # Canonical 10-task demo workflow runner
│   ├── export_test.go                   # Test hooks for TUI model
│   ├── interactive_test.go              # Unit tests for TUI lifecycle & state transitions
│   └── client_test.go                   # Tests for InMemoryClient, HTTPClient & workflow
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
level=INFO msg="server: starting background lists projector..."
level=INFO msg="server: starting HTTP gateway..." addr=:8080
```

To enable verbose debug logging for all HTTP requests and CQRS command/query dispatches:

```bash
go run ./cmd/server -addr :8080 -debug
```

Example debug output:
```text
level=DEBUG msg="server: http request received" method=POST path=/tasks query="" remote_addr=127.0.0.1:54321
level=DEBUG msg="server: executing AddTask command" cmd_id=urn:todo:... list_id=urn:todo:... task="Buy groceries"
level=DEBUG msg="server: AddTask command succeeded" cmd_id=urn:todo:... task="Buy groceries"
level=DEBUG msg="server: http request completed" method=POST path=/tasks status=201 duration=1.1ms
```

#### 2. Run Interactive Client (Default)

In a separate terminal:

```bash
cd example/todo
go run ./cmd/client -server http://localhost:8080
```

The interactive terminal client is built with [Bubble Tea](https://github.com/charmbracelet/bubbletea), [Bubbles](https://github.com/charmbracelet/bubbles), and [Lip Gloss](https://github.com/charmbracelet/lipgloss). It provides a full-terminal, scrollable TUI with real-time CQRS updates, fuzzy search, and keyboard-driven cycling across lists and tasks:

```text
┌────────────────────────────────────────────────────────┐
│            Screen 1: Lists Catalog (Entry)             │
│  - Shows all available Todo Lists & summary stats      │
│  - Cycle with ↑/↓ or j/k, fuzzy filter with /          │
│  - Create new list (a/n), or open selected list (enter)│
└───────────────────────┬────────────────────────────────┘
                        │
                        │ [Enter / o]  (Open selected list)
                        │ [Esc / b]    (Return to catalog)
                        ▼
┌────────────────────────────────────────────────────────┐
│             Screen 2: Task Detail Screen               │
│  - Shows active and completed/archived tasks for list  │
│  - Cycle with ↑/↓ or j/k, fuzzy filter with /          │
│  - Add task (a), complete (c/space), delete (d)        │
└────────────────────────────────────────────────────────┘
```

---

##### Screen 1: Todo Lists Catalog (Entry Screen)

When launched, the client displays the catalog of all known Todo List aggregates powered by the `lists` and `counter` read-model projections:

```text
 FLUX CQRS TODO APP   Global Stats: 5 Active | 2 Archived | 1 Removed

  Todo Lists Catalog
  2 lists

  > 1. Work Tasks
       Active: 3 • Archived: 1 • urn:todo:prod:lists:1:list:work-1234
    2. Personal Tasks
       Active: 2 • Archived: 1 • urn:todo:prod:lists:1:list:personal-5678

  enter/o open • a/n new list • r refresh • / filter • q quit
```

**Lists Screen Controls:**
* `↑` / `↓` or `k` / `j`: Cycle selection cursor through available todo lists
* `enter` or `o`: Open the selected list into Screen 2 (Tasks Screen)
* `a` or `n`: Open inline dialog to create a new Todo List (type title, `enter` to confirm, `esc` to cancel)
* `/`: Activate fuzzy filter to quickly search through lists
* `r`: Refresh lists and global stats from the server read models
* `q` or `ctrl+c`: Quit application

---

##### Screen 2: Task Detail Screen

Opening a list navigates into its tasks screen, displaying active and archived items:

```text
 FLUX CQRS TODO APP   Global Stats: 5 Active | 2 Archived | 1 Removed

  Tasks — Work Tasks
  4 items • urn:todo:prod:lists:1:list:work-1234

  > 1. [ACTIVE] Prepare release notes
       Pending
    2. [ACTIVE] Deploy staging cluster
       Pending
    3. [ACTIVE] Update documentation
       Pending
    4. [DONE] Initial architecture review
       Completed / Archived

  a add • c/space done • d delete • esc/b back • r refresh • / filter • q quit
```

**Tasks Screen Controls:**
* `↑` / `↓` or `k` / `j`: Cycle selection cursor up and down through tasks
* `a`: Open inline dialog to add a new task (type task name, `enter` to confirm, `esc` to cancel)
* `c` or `space`: Mark the selected task as completed / archived
* `d`: Delete / remove the selected task
* `esc` or `b`: Return back to Screen 1 (Todo Lists Catalog)
* `/`: Activate fuzzy filter to search tasks
* `r`: Refresh task list and stats from the server
* `q` or `ctrl+c`: Quit application

---

##### Real-Time Synchronization (SSE Push Notifications)

When multiple clients are connected simultaneously:
* Any domain mutation made by one client (e.g. creating a list, adding a task, completing a task, or deleting a task) writes an event to the Event Store.
* The server broadcasts the event to all connected clients via HTTP Server-Sent Events (`GET /events`).
* Competing clients immediately display a status toast (e.g. `⚡ Live: Tasks completed: "Prepare release notes"`) and automatically re-sync their active screen in real time without pressing `r`.

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
