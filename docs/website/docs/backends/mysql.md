# MySQL Backend

*(Planned)*

The MySQL backend will provide ACID-compliant event storage using relational tables.

## Planned Architecture
- **Append-Only Event Log:** Storing serialized envelopes with optimistic concurrency control using unique constraints on `(stream_id, revision)`.
- **Global Ordering:** A global auto-incrementing `position` column for sequential projector tailing.
- **Snapshot Storage:** A dedicated table for storing serialized aggregate snapshots to speed up rehydration.
