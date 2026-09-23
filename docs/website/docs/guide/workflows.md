# Workflows & Temporal

A **Workflow** (or Saga/Process Manager) coordinates long-running business processes that span multiple aggregates (e.g., placing an order, waiting for payment, and instructing shipping).

Because workflows are inherently stateful and frequently require durable timers ("if payment isn't confirmed in 10 minutes, cancel the order"), they are notoriously complex to build reliably.

## The Temporal Integration Strategy

`flux` was designed to perfectly integrate with [Temporal](https://temporal.io/) to handle durable executions. 

Rather than reinventing a state machine engine, we highly recommend delegating long-running orchestrations and background Projections to Temporal. 

The most robust architecture combines **EventStore Tailing** with **EventBus Signals**.

### 1. The Hybrid Tailing Workflow

If you want to build a continuous Projection (or a global process manager that listens to the entire event stream), the most resilient pattern is a Temporal workflow that runs in an infinite loop:

1. It fetches a batch of events starting from its `lastRevision`.
2. It executes a Temporal Activity to process the batch transactionally.
3. If it is caught up to the live stream, it blocks using a `workflow.Selector` waiting for a `WakeUpSignal`.
4. It includes a fallback timer (e.g., 1 minute) just in case a signal is lost.

```go
func DashboardProjectionWorkflow(ctx workflow.Context, lastRevision int) error {
	wakeupChan := workflow.GetSignalChannel(ctx, "WakeUpSignal")

	for {
		var batch []flux.Envelope
		var nextRevision int
		
		// 1. Fetch historical events from your EventStore
		err := workflow.ExecuteActivity(ctx, FetchEventsActivity, lastRevision).Get(ctx, &batch)
		if err != nil {
			return err
		}
		
		// 2. Process the batch (e.g., update a SQL read model)
		for _, env := range batch {
			err := workflow.ExecuteActivity(ctx, UpdateDashboardActivity, env).Get(ctx, nil)
			if err != nil {
				return err // Temporal retries automatically!
			}
			lastRevision = env.GlobalPosition
		}
		
		// 3. Prevent workflow history bloat
		if workflow.GetInfo(ctx).GetCurrentHistoryLength() > 10000 {
			return workflow.NewContinueAsNewError(ctx, DashboardProjectionWorkflow, lastRevision)
		}
		
		// 4. If caught up, go to sleep and wait for a signal
		if len(batch) == 0 {
			selector := workflow.NewSelector(ctx)
			
			// Listen for WakeUp signals
			selector.AddReceive(wakeupChan, func(c workflow.ReceiveChannel, more bool) {
				c.Receive(ctx, nil)
			})
			
			// 1-minute fallback polling
			timerCtx, cancelTimer := workflow.WithCancel(ctx)
			selector.AddFuture(workflow.NewTimer(timerCtx, 1 * time.Minute), func(f workflow.Future) {})
			
			selector.Select(ctx) // Blocks here!
			cancelTimer()
		}
	}
}
```

This completely eliminates database hammering during live operation while maximizing throughput during historical replays!

### 2. The EventBus Trigger

To wake the workflow up the millisecond a new event is saved, wire your `flux.EventBus` in `main.go` to send a Temporal signal:

```go
eventBus.RegisterGlobal(func(ctx event.Context, e flux.Event) error {
	// Signal the projector to wake up and fetch the new event
	return temporalClient.SignalWorkflow(
		context.Background(),
		"DashboardProjection", // The stable Workflow ID
		"",
		"WakeUpSignal",
		nil,
	)
})
```

## Implementing Sagas (Process Managers)

For business orchestrations that are triggered by a single event (e.g., "Order Placed"), you don't need a tailing loop.

1. **Triggering:** Use the `flux.EventBus` to start a new Temporal Workflow execution (`OrderFulfillmentWorkflow`).
2. **Execution:** The Temporal Workflow natively maintains its state and timers (e.g., waiting 10 minutes for payment confirmation).
3. **Command Dispatch:** When the Temporal Workflow decides to take action across the domain, it executes an Activity. That Activity should use the `flux.CommandBus` to execute a command against a Domain Aggregate.

```go
// A Temporal Activity dispatching a Flux Command
func PayOrderActivity(ctx context.Context, orderID string) error {
    // 1. Build the command context with traceability
    cmdCtx := command.NewContext(
		ctx, 
		flux.MustParseIdentifier("urn:cmd:pay-order:123"), 
		flux.Actor{}, // System actor
		flux.Identifier{}, 
		flux.Identifier{},
	)
	
	// 2. Dispatch the command into the flux ecosystem
    return command.Execute(cmdCtx, globalCmdBus, salesCmd.PayOrder{OrderID: orderID})
}
```
