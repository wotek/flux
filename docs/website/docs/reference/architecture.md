# Architecture & Boundaries

`flux` enforces strict boundaries to keep your application logic clean, scalable, and testable.

## 1. Event Sourcing
State is not stored; it is derived. The `EventStore` is the absolute source of truth. Aggregates are hydrated by replaying history. If a database is corrupted, you can wipe it and rebuild the entire system's state from the immutable event log.

## 2. CQRS
Commands (Write Model) and Queries (Read Model) are physically and logically separated. They use different buses, different data models, and scale independently.

## 3. Reflection-Free Generics
`flux` heavily leverages Go 1.20+ Generics to provide type safety without the massive performance overhead of `reflect`. Aggregate hydration and event decoding happen via explicitly registered factory functions and generic constraints.

## 4. Serialization Boundaries (The Edge)

`flux` enforces a strict rule regarding data serialization: **The core domain must remain completely unaware of how it is serialized.**

The core `flux.Envelope` contains an interface (`flux.Event`). Therefore, it cannot be natively passed into standard `json.Unmarshal` without failing. 

All infrastructure "Edges" (Database drivers, Kafka publishers, REST APIs) must define their own private **Data Transfer Objects (DTOs)**. The Edge is responsible for flattening the Envelope, converting the Event interface into raw bytes, and storing it. Upon retrieval, the Edge is responsible for using a Type Registry to map the event name string back to a concrete Go struct, unmarshaling the bytes, and rebuilding the pure `flux.Envelope` before handing it back to the domain.
