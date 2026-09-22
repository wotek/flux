# Events & Envelopes

In `flux`, domain events are the absolute source of truth. They represent facts that have already occurred within your system.

## Defining Events

An event is simply a Go struct that implements the `flux.Event` interface.

```go
type OrderPlaced struct {
    OrderID  string
    TotalAmount int
}

// Name returns the unique identifier for this event type.
func (e OrderPlaced) Name() string { return "OrderPlaced" }
```

## Envelopes

While your application logic deals exclusively with pure Domain Events, the `EventStore` wraps these events in an `Envelope`.

The `Envelope` contains crucial metadata for the framework:
- **GlobalPosition:** The absolute, sequentially ordered position of the event in the entire database (used for projection tailing).
- **Position:** The position of the event within its specific Stream (used for aggregate versioning and optimistic concurrency).
- **Actor:** Who performed the action.
- **CorrelationIdentifier:** Used to trace a workflow across multiple systems.

You will rarely interact with Envelopes directly unless you are building a custom backend or a Temporal tailing projection.
