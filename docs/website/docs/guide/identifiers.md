# Structured Identifiers (URNs)

To ensure global uniqueness, provide deep contextual meaning, and make distributed debugging effortless, `flux` relies heavily on **Uniform Resource Names (URNs)** for identifying streams, events, commands, and actors.

## The Identifier Type

Instead of passing around opaque strings or raw UUIDs, `flux` uses a strictly typed `Identifier` struct. 

```go
id := flux.MustParseIdentifier("urn:catalog:product:01ARZ3NDEKTSV4RRFFQ69G5FAV")
```

The URN format in `flux` strictly follows: `urn:<domain>:<resource_type>:<resource_id>`. By using this format, a developer looking at a database log instantly knows exactly what system, what entity, and what specific record triggered an event.

## The Resource ID: Why we Highly Recommend ULIDs

While `flux` allows you to use any string for the `<resource_id>` part of the URN (UUIDs, Integers, or custom strings), **we highly recommend using ULIDs** (Universally Unique Lexicographically Sortable Identifiers).

When designing an Event Sourcing system, you are designing a system that will append millions (or billions) of events over time. The ID generation strategy you choose has massive implications on your database performance.

Here is a comparison of the three most common ID strategies:

### 1. Auto-Incrementing Integers (`urn:catalog:product:123`)
* **Pros:** Highly readable, naturally sorted, zero database index fragmentation.
* **Cons:** Extremely difficult to generate in a highly concurrent distributed system without a centralized bottleneck. They also expose business metrics (competitors can guess how many orders you have) and invite IDOR security vulnerabilities.

### 2. UUID v4 (`urn:catalog:product:550e8400-e29b-41d4-a716-446655440000`)
* **Pros:** Cryptographically random, easily generated across distributed microservices with zero coordination.
* **Cons:** **Massive Index Fragmentation.** Because UUIDs are completely random, every new event inserted into your SQL/Mongo database inserts itself randomly into the middle of the B-Tree index. At millions of rows, this causes massive page faults, index rebuilding, and drastically slows down your database.

### 3. ULIDs (`urn:catalog:product:01ARZ3NDEKTSV4RRFFQ69G5FAV`) - 🌟 Recommended
* **Pros:** ULIDs combine the best of both worlds. The first 48 bits are a millisecond timestamp, and the remaining 80 bits are cryptographic randomness.
  * **Zero Coordination:** They can be generated safely across distributed systems.
  * **Zero Fragmentation:** Because they start with a timestamp, they are naturally chronologically sortable. Every new event inserted into your database is cleanly appended to the right-edge of your B-Tree index, keeping database inserts lightning-fast permanently.
  * **URL Safe:** They are Base32 encoded, meaning no hyphens or weird characters.

## Examples in Practice

### Using ULIDs for Streams (Aggregates)

When you create a new aggregate, you generate a ULID for it. Every event emitted by this aggregate will share this Stream Identifier.

```go
import "github.com/oklog/ulid/v2"

// Generate a new ULID (e.g., 01HN7XYWGF...)
resourceID := ulid.Make().String()

// Build the URN
stream := flux.Stream{
    Identifier: flux.MustParseIdentifier("urn:sales:order:" + resourceID),
}
```

### Using ULIDs for Events

By generating a unique ULID for every single Event you append to the database, you gain a massive superpower: **your raw events table is naturally sorted by time**. 

If you run `SELECT * FROM events ORDER BY id ASC`, your database doesn't even need to sort by a timestamp column; the string ID itself provides the perfect chronological order.

*(Note: The framework still relies on mathematical `uint64` counters for strict idempotency and optimistic concurrency, but ULID identifiers provide the underlying database optimization!)*

## Is it mandatory?

**No.** The `flux.MustParseIdentifier` function simply validates the URN structure (`urn:x:y:z`). The framework does not care what you place in the `z` portion. If you are migrating a legacy system that relies on UUIDs, `flux` will accept them without complaint!

## Serialization & JSON Compatibility

Because `Identifier` is a fundamental Value Object, it implements the Go standard library's `encoding.TextMarshaler` and `encoding.TextUnmarshaler` interfaces. 

This means that `flux.Identifier` works completely natively with `encoding/json`, XML, YAML, and most database ORMs right out of the box, cleanly serializing to and from its string URN representation without any custom logic on your end.

```go
type APIResponse struct {
    UserID flux.Identifier `json:"user_id"`
    Name   string          `json:"name"`
}

// When marshaled, it automatically flattens to a string!
// {"user_id": "urn:auth:user:01HN7...", "name": "John"}
json.Marshal(response)
```

Furthermore, because `UnmarshalText` internally relies on `flux.ParseIdentifier()`, if a frontend client sends malformed JSON (e.g., `{"user_id": "invalid-uuid"}`), the standard `json.Unmarshal` process will automatically fail and reject the payload, protecting your domain!
