# Architecture & Boundaries

`flux` enforces strict boundaries to keep your application logic clean, scalable, and testable.

## 1. Event Sourcing
State is not stored; it is derived. The `EventStore` is the absolute source of truth. Aggregates are hydrated by replaying history. If a database is corrupted, you can wipe it and rebuild the entire system's state from the immutable event log.

## 2. CQRS
Commands (Write Model) and Queries (Read Model) are physically and logically separated. They use different buses, different data models, and scale independently.

## 3. Reflection-Free Generics
`flux` heavily leverages Go 1.20+ Generics to provide type safety without the massive performance overhead of `reflect`. Aggregate hydration and event decoding happen via explicitly registered factory functions and generic constraints.
