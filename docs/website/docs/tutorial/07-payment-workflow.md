# 7. The Payment Workflow (Process Managers)

In Chapter 5, we built the `Order` aggregate in the Sales Domain. When an order is placed, we need to charge the customer's credit card. 

However, the Sales Domain should not know how to charge credit cards—that belongs to an external Payment Gateway. Furthermore, charging a card takes time, it can fail, it can time out, and if it fails permanently, we need to tell the Sales Domain to cancel the Order.

This requires a **Workflow** (often called a Saga or Process Manager).

## The Complexity of Distributed Sagas

Writing state machines in Go to manage distributed sagas is notoriously difficult. You have to handle database polling, durable timers ("if payment takes longer than 10 minutes, cancel"), and compensating transactions, all while surviving application crashes.

To solve this, `flux` relies heavily on [Temporal](https://temporal.io/).

## The Hybrid Tailing Strategy

The most robust way to integrate `flux` with Temporal is a hybrid **Tailing & Signaling** architecture. 

If a Temporal Workflow simply polls the `EventStore` every second, it will hammer your database and destroy performance. Instead, we use the `flux.EventBus` to gently wake the Workflow up.

### 1. The Activity (Tailing)

First, we write a Temporal Activity that fetches new events from the `EventStore`.

```go
// FetchEventsActivity fetches a batch of events starting from the provided global position.
func FetchEventsActivity(ctx context.Context, startPosition uint64) ([]flux.Envelope, error) {
    // Queries the EventStore for events WHERE global_position > startPosition
    return eventStore.LoadGlobal(ctx, startPosition, 100)
}
```

### 2. The Workflow (Orchestrator)

Next, we write the Temporal Workflow. It loops continuously, fetching events. 

- If it finds events (`len(batch) > 0`), it processes them immediately to catch up.
- If it finds NO events (`len(batch) == 0`), it uses a Temporal `workflow.Selector` to block and wait for a `WakeUpSignal`.

```go
func OrderProcessManager(ctx workflow.Context) error {
    position := uint64(0)
    
    // Create a signal channel for wake-ups
    wakeUpCh := workflow.GetSignalChannel(ctx, "WakeUpSignal")

    for {
        var events []flux.Envelope
        
        // 1. Fetch events from the database
        err := workflow.ExecuteActivity(ctx, FetchEventsActivity, position).Get(ctx, &events)
        if err != nil {
            return err
        }

        // 2. Catching Up: If we found events, process them immediately!
        if len(events) > 0 {
            for _, env := range events {
                if env.Event.Name() == "OrderPlaced" {
                    // Dispatch a Command to the Payment system!
                }
                position = env.GlobalPosition
            }
            continue // Loop immediately without sleeping
        }

        // 3. Live Mode: No new events. Wait for a signal OR a fallback timer.
        selector := workflow.NewSelector(ctx)
        
        // Listen for the WakeUp signal
        selector.AddReceive(wakeUpCh, func(c workflow.ReceiveChannel, more bool) {
            c.Receive(ctx, nil) // Consume the signal
        })
        
        // Fallback timer (e.g., 1 minute) just in case a signal is dropped
        selector.AddFuture(workflow.NewTimer(ctx, time.Minute), func(f workflow.Future) {})
        
        // Block until either the Signal arrives or the Timer fires
        selector.Select(ctx)
    }
}
```

### 3. The Event Bus Signal

Finally, how do we trigger that `WakeUpSignal`? In our standard Go application, we simply listen to the `EventBus` and blindly send a signal to Temporal whenever *any* event occurs!

```go
event.Register(eventBus, func(ctx event.Context, e any) error {
    // Send signal to Temporal
    return temporalClient.SignalWorkflow(ctx, workflowID, "", "WakeUpSignal", nil)
})
```

### Why this is brilliant:
1. **Zero Database Hammering:** The workflow sleeps completely when the system is idle.
2. **Lightning Fast Catch-ups:** When thousands of events are pouring in, the workflow completely ignores the signals and just loops the Activity continuously, maximizing throughput.
3. **Durable Timers:** Temporal natively handles compensating actions (like dispatching a `CancelOrderCommand` if the payment is still pending after 24 hours).

---

Congratulations! You have successfully completed the comprehensive `flux` tutorial. You now understand how to build Aggregates, enforce Invariants, route Commands, project Read Models, and orchestrate complex distributed Workflows!
