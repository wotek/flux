# CQRS Buses

`flux` enforces strict Command Query Responsibility Segregation (CQRS) through three distinct message buses. You should never mix Intents, Facts, and Data Retrieval.

## Command Bus (1:1)
Commands are **Intents** to change system state. They are dispatched to exactly **one** handler. If no handler exists, or multiple exist, it is an error.

```go
cmdBus := command.New()
command.Register(cmdBus, myHandler)

// Dispatching
command.Execute(ctx, cmdBus, AddItemCommand{ID: "123"})
```

## Event Bus (1:N)
Events represent **Facts** that have already occurred. They are dispatched to **many** handlers (Pub/Sub). Projections and Workflows listen to this bus.

```go
eventBus := event.New()
event.Register(eventBus, myProjectionHandler)
event.Register(eventBus, myWorkflowHandler)

// Publishing
eventBus.Publish(ctx, ItemAdded{ID: "123"})
```

## Query Bus (1:1 with Result)
Queries request data without modifying state. They are routed to exactly **one** handler and return a typed result.

```go
queryBus := query.New()
query.RegisterHandler(queryBus, myQueryHandler)

// Asking
result := queryBus.Ask(ctx, GetItemQuery{ID: "123"})
```

## Middleware Chaining (Interceptors)

All three dispatchers (`command`, `query`, and `event`) natively support middleware chaining. 

This allows developers to inject cross-cutting concerns like global telemetry, authentication barriers, database transaction management, and OpenTelemetry spans without polluting domain logic.

Middlewares are registered using the `Use()` method:

```go
bus.Use(func(ctx command.Context, cmd any, next func(command.Context, any) error) error {
    ctx.Logger().Info("Executing command", "type", fmt.Sprintf("%T", cmd))
    
    // Call the next middleware in the chain (or the final handler)
    err := next(ctx, cmd)
    
    return err
})
```

Middlewares execute in the exact order they are provided, chaining perfectly down to the underlying handler.
