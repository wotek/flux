# Redis Backend

*(Planned)*

Redis is an exceptional target for **Projections (Read Models)** due to its sub-millisecond query performance.

## Planned Use Cases
- **Key-Value Projections:** Projecting aggregate states into simple Redis keys for lighting-fast UI lookups.
- **Snapshot Caching:** Using Redis as a transient Snapshot store to speed up Aggregate hydration without hitting the primary disk-based EventStore.
