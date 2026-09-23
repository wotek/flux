# Todo CLI Application

The `example/todo` application is a fully functional, production-ready CQRS application. 

It features an HTTP server powered by `flux`, and an interactive terminal UI (TUI) client built with Bubble Tea.

## What it demonstrates

This application is designed to teach you how to build a real-world system using the standard `flux` project layout.

1. **Strict CQRS:** The Write models (Aggregates) and Read models (Projections) are completely separated.
2. **HTTP API:** How to expose the `flux.CommandBus` and `flux.QueryBus` over REST endpoints.
3. **Real-Time Projections:** How to listen to the `flux.EventBus` and update an SQLite or in-memory Read Model.
4. **Server-Sent Events (SSE):** How the server broadcasts Domain Events in real-time to connected clients.
5. **Interactive UI:** A beautiful terminal client that reacts instantly to state changes.

## Running the Example

This example requires you to run both a server and a client.

**1. Start the Server:**
```bash
cd example/todo
go run ./cmd/server
```

**2. Start the Interactive CLI Client:**
Open a new terminal window:
```bash
cd example/todo
go run ./cmd/todo
```

## Source Code Highlights

Take a deep dive into the `example/todo/internal/todo` package to see the architecture in action:

*   **`aggregates/`**: Look at `list.go` to see how Todo lists manage their own state and emit `TaskAdded` or `TaskCompleted` events.
*   **`projections/`**: Look at `stats/projector.go` to see how the system builds a blazing-fast global Read Model (tracking total active vs archived tasks) by listening to events across all lists!
*   **`api/`**: See how HTTP routes are bound directly to the `commandBus` and `queryBus`.
