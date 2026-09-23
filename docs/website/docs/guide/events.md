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

## The Serialization Problem (DTOs at the Edge)

If you look closely at the `flux.Envelope` struct, you will notice it contains `Event flux.Event`. 

Because `flux.Event` is a Go interface, **an Envelope cannot be natively deserialized by standard `encoding/json`**. Go has no idea which concrete struct (e.g., `OrderPlaced` vs `ItemAdded`) to instantiate just by looking at a JSON payload.

### The DTO Pattern
Because of this, `flux` Envelopes are treated strictly as **in-memory domain concepts**. 

Any boundary that touches the outside world—whether it's the `EventStore` saving to Redis, or a message broker publishing to Kafka—**must use Data Transfer Objects (DTOs)** and a Type Registry.

```go
// Example of a private DTO inside a Redis driver
type envelopeDTO struct {
	ID             string          `json:"id"`
	EventName      string          `json:"event_name"`
	EventPayload   json.RawMessage `json:"event_payload"`
	// ...
}
```

When a storage driver saves an Envelope, it maps the core Envelope into a flat DTO and uses `json.Marshal(env.Event)` to convert the interface payload into raw bytes.

When it loads an Envelope, it inspects the `dto.EventName` string, looks it up in a Registry to get an empty struct (e.g., `&OrderPlaced{}`), unmarshals the `EventPayload` into that struct, and then reassembles the core `flux.Envelope`.

This boundary guarantees that the core framework remains utterly unaware of JSON, XML, or database serialization concerns!
