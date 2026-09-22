# Workflows & Temporal

A **Workflow** (or Saga/Process Manager) coordinates long-running business processes that span multiple aggregates (e.g., placing an order, waiting for payment, and instructing shipping).

Because workflows are inherently stateful and frequently require durable timers ("if payment isn't confirmed in 10 minutes, cancel the order"), they are notoriously complex to build reliably.

## Temporal Integration

`flux` perfectly integrates with [Temporal](https://temporal.io/) to handle these durable executions. The most robust architecture combines **EventStore Tailing** with **EventBus Signals**:

1. A Temporal Workflow tails the `flux` EventStore using an Activity loop.
2. It executes state mutations transactionally.
3. Once caught up to the live stream, it blocks using a `workflow.Selector` waiting for a `WakeUpSignal`.
4. Your application's `flux.EventBus` simply sends a `WakeUpSignal` to Temporal whenever a new event occurs.

This completely eliminates database hammering during live operation while maximizing throughput during replays!
