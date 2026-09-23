# Coding Agent Guide: Flux + Temporal Integration

This document provides instructions and code patterns for AI coding agents tasked with building a demo application using the `github.com/wotek/flux` Event Sourcing & CQRS framework, integrated with Temporal.

## 1. Core Framework Rules

`flux` is a highly-opinionated, generics-based framework. When generating code, you MUST follow these patterns exactly:

*   **Events:** Must implement `flux.Event` (provide a `Name() string` method).
*   **Aggregates:** Must embed `flux.AggregateRoot[flux.Event]`. You must define a `New(stream flux.Stream) *MyAggregate` factory and an `apply(event flux.Event) error` state mutator.
*   **Mutations:** Aggregates mutate their state *only* by calling `a.Changeset().Record(MyEvent{})`. Do NOT directly mutate state inside command handlers.
*   **Repositories:** Use `flux.NewAggregateRepository[*MyAggregate, flux.Event](eventStore)` to save and load aggregates.
*   **Buses:** Use `command.New()` and `event.New()` for routing.

## 2. Implementing Temporal Projections (Read Models)

Do NOT use the built-in `flux/projection` package. Instead, implement Projections as continuous Temporal Workflows that use a hybrid tailing/signaling approach:

### The Projection Workflow
Create a Temporal Workflow that tails the EventStore.

```go
func DashboardProjectionWorkflow(ctx workflow.Context, lastRevision int) error {
	wakeupChan := workflow.GetSignalChannel(ctx, "WakeUpSignal")

	for {
		var batch []flux.Envelope
		var nextRevision int
		
		// FetchEventsActivity should wrap eventStore.Read(ctx, flux.Stream{}, lastRevision, batchSize)
		err := workflow.ExecuteActivity(ctx, FetchEventsActivity, lastRevision).Get(ctx, &batch)
		if err != nil {
			return err
		}
		
		for _, env := range batch {
			// UpdateDashboardActivity executes your SQL INSERTs
			err := workflow.ExecuteActivity(ctx, UpdateDashboardActivity, env).Get(ctx, nil)
			if err != nil {
				return err // Temporal retries automatically
			}
			lastRevision = env.GlobalPosition
		}
		
		if workflow.GetInfo(ctx).GetCurrentHistoryLength() > 10000 {
			return workflow.NewContinueAsNewError(ctx, DashboardProjectionWorkflow, lastRevision)
		}
		
		// If caught up to the live stream, block until the EventBus signals us
		if len(batch) == 0 {
			selector := workflow.NewSelector(ctx)
			selector.AddReceive(wakeupChan, func(c workflow.ReceiveChannel, more bool) {
				c.Receive(ctx, nil)
			})
			
			// 1-minute fallback polling
			timerCtx, cancelTimer := workflow.WithCancel(ctx)
			selector.AddFuture(workflow.NewTimer(timerCtx, 1 * time.Minute), func(f workflow.Future) {})
			
			selector.Select(ctx)
			cancelTimer()
		}
	}
}
```

### The EventBus Trigger
In your `main.go`, wire the `flux` EventBus to signal the Temporal Workflow whenever an event is persisted:

```go
event.RegisterGlobal(eventBus, func(ctx event.Context, e flux.Event) error {
	// Signal the projector to wake up and fetch the new event
	return temporalClient.SignalWorkflow(
		context.Background(),
		"DashboardProjection", // The workflow ID
		"",
		"WakeUpSignal",
		nil,
	)
})
```

## 3. Implementing Long-Running Process Managers (Sagas)

Do NOT use the built-in `flux/workflow` or `flux/saga` packages. Implement them natively in Temporal.

1.  **Triggering:** When a domain event occurs that starts a process (e.g., `OrderPlaced`), use the `EventBus` to start a new Temporal Workflow execution.
2.  **Execution:** The Temporal Workflow natively maintains its state and timers (e.g., waiting 10 minutes for payment).
3.  **Command Dispatch:** When the Temporal Workflow decides to take action, it executes an Activity. That Activity should use the `command` bus to execute a command against a Domain Aggregate.

```go
// Temporal Activity dispatching a Flux Command
func PayOrderActivity(ctx context.Context, orderID string) error {
    cmdCtx := command.NewContext(ctx, flux.MustParseIdentifier("urn:cmd..."), actor, corrID, causID)
    return command.Execute(cmdCtx, globalCmdBus, salescmd.PayOrder{OrderID: orderID})
}
```

## Task

Using the patterns above, set up a minimal demo application (e.g., an Order & Payment flow) that provisions an EventStore, processes an Order through a Temporal Process Manager, and projects the final state using the hybrid Temporal Projection tailing workflow.
