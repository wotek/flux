# Redis Event Store Implementation Plan

## Overview
This document outlines the implementation plan for adding a **Redis backend** to `flux`. Redis will be used primarily for fast prototyping and as an ephemeral cache/fast-path for the Event Store and Snapshot Store.

## 1. Storage Requirements
The `flux.EventStore` interface dictates three core capabilities:
1. **Append with Optimistic Concurrency:** Appending an event to a specific stream must fail if the expected revision does not match.
2. **Stream Reading:** Fetching all events for a specific stream in order.
3. **Global Tailing:** Fetching events sequentially across *all* streams (using `GlobalPosition`).

---

## 2. Architecture: Native Redis Streams with Custom IDs

We will use native Redis Streams (`XADD` / `XREAD`) to gain the massive performance benefits of blocking reads, while strictly maintaining `flux`'s `uint64` interface.

### The Stream ID Trick
By default, Redis auto-generates Stream IDs using the format `<millisecondsTime>-<sequenceNumber>` (e.g., `1526919030474-55`). 

However, **Redis allows you to provide explicit IDs**, so long as they are strictly increasing. We will bypass time-based IDs entirely. We will maintain standard integer counters using `INCR`, and use them as the explicit IDs for our streams (e.g., `1-0`, `2-0`, `3-0`).

### ULIDs vs. Sequential Integers
The `flux` framework heavily utilizes **ULIDs** (Universally Unique Lexicographically Sortable Identifiers) for both Stream Identifiers (Aggregates) and Event Identifiers. 

While ULIDs are incredibly powerful for database clustering and natural chronological sorting, they do not guarantee mathematical continuity (you cannot detect if an event was skipped between two ULIDs). Therefore, the architecture cleanly separates these concerns:
*   **The Payload:** The ULID remains the globally unique identifier for the event and the stream, stored inside the serialized JSON envelope.
*   **The Stream ID:** The strictly contiguous `uint64` counters are used natively as the Redis Stream IDs to enforce CQRS optimistic concurrency and ensure projectors never miss an event.

### Keys Used
* `position:global` (String: Tracks the global `uint64` position)
* `revision:{urn}` (String: Tracks the aggregate's `uint64` revision)
* `stream:{urn}` (Stream: The aggregate's event log)
* `stream:global` (Stream: The global event log for tailing)

### Mechanism (Atomic Lua Script)
When `Save()` is called, we execute a Lua script to guarantee ACID-like consistency:
1. Fetch `current_version` from `revision:{urn}`.
2. If `current_version != expected_version`, return `flux.ErrConcurrency`.
3. `new_version = INCR revision:{urn}`.
4. `new_global = INCR position:global`.
5. `XADD stream:{urn} {new_version}-0 payload {JSON}`.
6. `XADD stream:global {new_global}-0 payload {JSON}`.

### Decoding (`XREAD`)
When projectors tail the global stream using `XREAD BLOCK`, Redis will return entries with IDs like `"4205-0"`. The Go Redis client will parse this, strip the `-0` suffix, convert the string back to a `uint64`, and map it perfectly to `Envelope.GlobalPosition`!

---

## 3. Snapshot Storage

* **Key Pattern:** `snapshot:{urn}`
* **Data Structure:** Simple String.
* **Mechanism:** `SET snapshot:{urn} {JSON_payload}`. 

---

## 4. Implementation Steps (For the Coding Agent)

1. **Lua Scripts:** Write the atomic append script and embed it using Go's `//go:embed`.
2. **Redis Client:** Use `github.com/redis/go-redis/v9`.
3. **EventStore Implementation:** 
   * Implement `eventstore.Store`.
   * Use `XREAD` for stream fetching.
   * Provide parsing utilities to convert Redis Stream IDs (`"X-0"`) to `uint64`.
4. **SnapshotStore Implementation:** Implement `flux.SnapshotStore` using basic `SET`/`GET`.
5. **Testing:** Run the existing `eventstore` compliance/contract tests against a real Docker Redis instance or `miniredis` to verify optimistic concurrency and tailing logic.

## 5. Serialization & Core Integrity (Critical Rule)

**Do NOT modify any core `flux` packages or components (e.g., `identifier.go`, `envelope.go`) to support Redis serialization.**

Because `flux.Identifier` uses unexported fields (like `urn string`), it natively fails `json.Marshal`. You might be tempted to add `MarshalJSON` or `MarshalText` methods directly to the core `flux.Identifier` struct. **Do not do this.** The core framework should remain entirely unaware of serialization concerns.

### The Solution: Private Data Transfer Objects (DTOs)
The Redis storage implementation must define its own private DTO structs to handle serialization entirely within the `event/store/redis/` package boundary.

**Example Design:**
```go
// internal/event/store/redis/dto.go

type envelopeDTO struct {
	ID             string            `json:"id"`
	EventName      string            `json:"event_name"`
	EventPayload   json.RawMessage   `json:"event_payload"`
	Revision       uint64            `json:"revision"`
	GlobalPosition uint64            `json:"global_position"`
	Actor          string            `json:"actor,omitempty"`
	Stream         string            `json:"stream"`
	CreatedAt      time.Time         `json:"created_at"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// toDTO converts a core flux.Envelope into the Redis-specific DTO.
// Notice how it extracts the string from the Identifier using .String()
func toDTO(env flux.Envelope) (envelopeDTO, error) {
	payload, err := json.Marshal(env.Event)
	if err != nil {
		return envelopeDTO{}, err
	}

	actorStr := ""
	if env.Actor.String() != "" { // Check if actor exists
		actorStr = env.Actor.String()
	}

	return envelopeDTO{
		ID:             env.Identifier.String(),
		EventName:      env.Event.Name(),
		EventPayload:   payload,
		Revision:       env.Revision,
		GlobalPosition: env.Position,
		Actor:          actorStr,
		Stream:         env.Stream.Identifier.String(),
		CreatedAt:      env.CreatedAt,
		Metadata:       env.Metadata,
	}, nil
}

// fromDTO reconstructs the core flux.Envelope.
// Notice how it uses flux.ParseIdentifier() to rebuild the core types.
func fromDTO(dto envelopeDTO, registry EventRegistry) (flux.Envelope, error) {
	id, err := flux.ParseIdentifier(dto.ID)
	streamID, err := flux.ParseIdentifier(dto.Stream)
    
	// ... parse Actor and reconstruct Event from registry using dto.EventName ...
	
	return flux.Envelope{
		Identifier: id,
		Stream:     flux.Stream{Identifier: streamID},
		Revision:   dto.Revision,
		Position:   dto.GlobalPosition,
		CreatedAt:  dto.CreatedAt,
		// Event, Actor, Metadata...
	}, nil
}
```

By mapping the core structures into a string-based `envelopeDTO` immediately before calling `json.Marshal`, and doing the reverse with `flux.ParseIdentifier` upon `json.Unmarshal`, all parsing logic remains strictly isolated to the Redis driver.

## 6. Directory Encapsulation (Strict Rule)

**Do NOT create top-level integration folders (e.g., `/redis/`).** 

While it might seem convenient to bundle the Event Store and Snapshot Store into a single top-level `redis` package, this pollutes the framework's root directory and breaks encapsulation. 

All backend driver implementations must be strictly localized to their respective conceptual directories:
*   The Redis Event Store must live entirely inside `internal/event/store/redis/` (or `event/store/redis/` if public).
*   If you implement a Snapshot Store, it must live entirely inside a localized directory (e.g., `snapshot/store/redis/`).

Any existing files in a top-level `redis/` directory must be moved to their proper encapsulated locations, and the top-level directory must be deleted.
