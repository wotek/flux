# PostgreSQL Backend

The PostgreSQL backend provides ACID-compliant event storage using relational tables in PostgreSQL.

`flux` includes a native PostgreSQL driver for the Event Store:
- **Event Store:** `github.com/wotek/flux/event/store/postgres`

::: info Event Store Scope
The PostgreSQL backend currently provides the **Event Store**. Snapshot, checkpoint, and projection stores can continue using MySQL, Redis, or in-memory implementations as applicable.
:::

## Event Store

The PostgreSQL Event Store persists events into a relational schema with decomposed columns for indexing, observability, and auditability. Appends are executed within a database transaction, guaranteeing atomic batch writes and strict optimistic concurrency control. Point-read retrieval of a single event envelope by stream and event identifier is provided via `Find(ctx, stream, eventID)`.

### Relational Schema

```sql
CREATE TABLE IF NOT EXISTS events (
    position BIGSERIAL PRIMARY KEY,
    stream_id TEXT NOT NULL,
    stream_type TEXT NULL,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    revision BIGINT NOT NULL, -- non-negative invariant: revision >= 0
    event_data BYTEA NOT NULL,
    causation_id TEXT NULL,
    correlation_id TEXT NULL,
    timestamp TIMESTAMPTZ(6) DEFAULT now() NOT NULL,
    application_metadata JSONB NULL,

    -- Optimistic Concurrency Constraint
    CONSTRAINT uk_stream_revision UNIQUE (stream_id, revision),

    -- Point Lookup and Idempotency Constraint
    CONSTRAINT uk_stream_event UNIQUE (stream_id, event_id)
);

-- Observability Indexes
CREATE INDEX IF NOT EXISTS idx_event_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_correlation ON events (correlation_id);
```

Raw SQL schema definitions are provided alongside the package in `event/store/postgres/schema.sql`. Schema creation is decoupled from application runtime, allowing DBAs and migration tools (e.g., `golang-migrate` or `goose`) full control over database initialization and indexes. The `CONSTRAINT uk_stream_event UNIQUE (stream_id, event_id)` constraint ensures that event IDs are unique within a stream and provides $O(1)$ index lookups for point-reads using `Find(ctx, stream, eventID)`.

### Optimistic Concurrency Control

Optimistic concurrency control is enforced through two complementary mechanisms:
1. **Row-level tip revision lock during append:** The store locks the current stream tip using `SELECT revision FROM events WHERE stream_id = $1 ORDER BY revision DESC LIMIT 1 FOR UPDATE` inside an active transaction (handling empty streams via `sql.ErrNoRows` as revision 0). If the current stream revision does not match `expectedRevision`, the transaction aborts with `flux.ErrConcurrency`.
2. **Database unique constraint:** The `CONSTRAINT uk_stream_revision UNIQUE (stream_id, revision)` ensures that even if concurrent transactions race on an empty or existing stream, any duplicate revision insert fails with SQLSTATE `23505` (unique violation), which is mapped to `flux.ErrConcurrency`.

### Driver & Connection

The store receives a standard Go `*sql.DB`. Connect to PostgreSQL using `database/sql` with the [`jackc/pgx`](https://github.com/jackc/pgx) driver:

```go
import (
	"database/sql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

db, err := sql.Open("pgx", "postgres://user:password@localhost:5432/flux?sslmode=disable")
```

### Codec Integration and Usage

The PostgreSQL Event Store is decoupled from serialization formats by accepting any `codec.Serializer` (`codec/json`, `codec/xml`, or `codec/protobuf`):

```go
package main

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/wotek/flux"
	jsoncodec "github.com/wotek/flux/codec/json"
	"github.com/wotek/flux/event"
	postgresstore "github.com/wotek/flux/event/store/postgres"
)

type AccountCreated struct {
	AccountID string `json:"account_id"`
	Owner     string `json:"owner"`
}

func (e AccountCreated) Name() string {
	return "AccountCreated"
}

func main() {
	db, err := sql.Open("pgx", "postgres://user:password@localhost:5432/flux?sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	registry := event.NewTypes()
	event.RegisterType[AccountCreated](registry)
	serializer := jsoncodec.New(registry)

	store := postgresstore.New(
		db,
		serializer,
		postgresstore.WithTableName("events"),
		postgresstore.WithBatchSize(100),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:banking:tenant-1:account:acc-123"),
	}

	env := flux.Envelope{
		Identifier:            flux.MustParseIdentifier("urn:acme:prod:banking:tenant-1:event:evt-001"),
		Event:                 AccountCreated{AccountID: "acc-123", Owner: "Alice"},
		Revision:              1,
		CreatedAt:             time.Now().UTC(),
		CorrelationIdentifier: flux.MustParseIdentifier("urn:acme:prod:banking:tenant-1:cmd:cmd-001"),
		CausationIdentifier:   flux.MustParseIdentifier("urn:acme:prod:banking:tenant-1:cmd:cmd-001"),
		Metadata:              map[string]string{"source": "web"},
	}

	if err := store.Append(ctx, stream, 0, []flux.Envelope{env}); err != nil {
		panic(err)
	}
}
```
