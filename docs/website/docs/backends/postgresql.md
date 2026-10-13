# PostgreSQL Backend

The PostgreSQL backend provides ACID-compliant event storage, aggregate snapshotting, stream checkpointing, and transactional projection updates using relational tables in PostgreSQL.

`flux` includes native PostgreSQL drivers for:
- **Event Store:** `github.com/wotek/flux/event/store/postgres`
- **Snapshot Store:** `github.com/wotek/flux/snapshot/store/postgres`
- **Checkpoint Store:** `github.com/wotek/flux/checkpoint/store/postgres`
- **Projection Store:** `github.com/wotek/flux/projection/store/postgres`

::: info Backend Scope
The PostgreSQL backend provides complete, production-ready storage implementations for the **Event Store**, **Snapshot Store**, **Checkpoint Store**, and **Projection Store**.
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

## Snapshot Store

The PostgreSQL Snapshot Store provides state caching by persisting serialized aggregate state along with revision metadata:

- **Package:** `github.com/wotek/flux/snapshot/store/postgres`

### Snapshot Schema

```sql
CREATE TABLE IF NOT EXISTS snapshots (
    stream_id TEXT PRIMARY KEY,
    revision BIGINT NOT NULL, -- non-negative invariant: revision >= 0
    snapshot BYTEA NOT NULL,
    timestamp TIMESTAMPTZ(6) NOT NULL DEFAULT now()
);
```

Raw SQL schema definitions are provided alongside the package in `snapshot/store/postgres/schema.sql`.

### Monotonic Upsert Semantics

Snapshots are saved using an `INSERT ... ON CONFLICT (stream_id) DO UPDATE SET` query that enforces monotonic revision semantics:

```sql
INSERT INTO snapshots AS s (stream_id, revision, snapshot, timestamp)
VALUES ($1, $2, $3, $4)
ON CONFLICT (stream_id) DO UPDATE SET
  revision = EXCLUDED.revision,
  snapshot = EXCLUDED.snapshot,
  timestamp = EXCLUDED.timestamp
WHERE EXCLUDED.revision >= s.revision;
```

Older revisions are silently ignored and do not overwrite newer cached snapshots.

### Usage

```go
package main

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/wotek/flux"
	postgressnapshot "github.com/wotek/flux/snapshot/store/postgres"
)

type AccountState struct {
	Balance int `json:"balance"`
}

func main() {
	db, err := sql.Open("pgx", "postgres://user:password@localhost:5432/flux?sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	snapshotStore := postgressnapshot.New[AccountState](
		db,
		postgressnapshot.WithTableName("snapshots"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:banking:tenant-1:account:acc-123"),
	}

	snap := flux.Snapshot[AccountState]{
		Revision: 10,
		State:    AccountState{Balance: 500},
	}

	if err := snapshotStore.Save(ctx, stream, snap); err != nil {
		panic(err)
	}

	loaded, err := snapshotStore.Load(ctx, stream)
	if err != nil {
		panic(err)
	}
	_ = loaded
}
```

## Checkpoint Store

The PostgreSQL Checkpoint Store persists the last successfully processed global event-stream position for tailing consumers such as `projection.Projector` and `workflow.Orchestrator`.

- **Package:** `github.com/wotek/flux/checkpoint/store/postgres`

### Relational Schema

```sql
CREATE TABLE IF NOT EXISTS checkpoints (
    consumer_id TEXT NOT NULL,
    position BIGINT NOT NULL, -- non-negative invariant: position >= 0
    updated_at TIMESTAMPTZ(6) NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_id)
);
```

The `consumer_id` column stores the full string representation of the consumer's `flux.Identifier` (e.g. `urn:acme:prod:projector:1:worker:lists`). Raw SQL schema definitions are provided alongside the package in `checkpoint/store/postgres/schema.sql`.

### Monotonic Max Semantics

Checkpoints are updated using an `INSERT ... ON CONFLICT (consumer_id) DO UPDATE SET` query that enforces monotonic max semantics:

```sql
INSERT INTO checkpoints AS c (consumer_id, position)
VALUES ($1, $2)
ON CONFLICT (consumer_id) DO UPDATE SET
  position = CASE
    WHEN EXCLUDED.position >= c.position THEN EXCLUDED.position
    ELSE c.position
  END,
  updated_at = CASE
    WHEN EXCLUDED.position >= c.position THEN now()
    ELSE c.updated_at
  END;
```

If an at-least-once retry or delayed worker attempts to commit an older position, PostgreSQL leaves the existing higher position intact. Equal or higher positions advance or refresh `updated_at`. Using the table alias `AS c` ensures unambiguous column qualification for both standard and schema-qualified table names.

### Usage

```go
package main

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/wotek/flux"
	checkpointpostgres "github.com/wotek/flux/checkpoint/store/postgres"
)

func main() {
	db, err := sql.Open("pgx", "postgres://user:password@localhost:5432/flux?sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	checkpointStore := checkpointpostgres.New(
		db,
		checkpointpostgres.WithTableName("checkpoints"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	consumerID := flux.MustParseIdentifier("urn:acme:prod:workflow:1:orchestrator:payment")

	if err := checkpointStore.SetPosition(ctx, consumerID, 1200); err != nil {
		panic(err)
	}

	position, err := checkpointStore.GetPosition(ctx, consumerID)
	if err != nil {
		panic(err)
	}
	_ = position
}
```

## Projection Store (Transactional Update)

The PostgreSQL Projection Store runs read-model SQL mutations and consumer checkpoint position updates in a **single atomic database transaction**.

- **Package:** `github.com/wotek/flux/projection/store/postgres`

### Transactional Atomicity vs. Best-Effort

| Approach | Architecture | Atomicity Guarantee |
|----------|--------------|---------------------|
| **Same-Database PostgreSQL Store** (`projection/store/postgres`) | Read-model tables and checkpoints table live in the **same** PostgreSQL database instance | **ACID Transactional:** Mutation and checkpoint commit together in one transaction. If mutation fails, rollback prevents checkpoint advance. |
| **Cross-Database / Composed Store** | Read-model in Redis / Elasticsearch / other DB; checkpoint in PostgreSQL / Redis | **Best-Effort:** Application mutates external read model, then calls `checkpoint.Store.SetPosition`. Crash between mutation and position save causes at-least-once redelivery. |

### Schema Requirement

The projection store uses the standard `checkpoints` table. Refer to [`checkpoint/store/postgres/schema.sql`](https://github.com/wotek/flux/blob/main/checkpoint/store/postgres/schema.sql) for the table DDL definition.

### Usage with `TxFromContext`

When `Projector` processes an envelope, `Store.Update` begins a PostgreSQL transaction and injects it into the context passed to registered event handlers. Retrieve the transaction using `postgresproj.TxFromContext(ctx)`:

```go
package main

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/wotek/flux"
	"github.com/wotek/flux/projection"
	postgresproj "github.com/wotek/flux/projection/store/postgres"
)

type ProductCreated struct {
	ProductID string
	Name      string
	Price     int
}

func (e ProductCreated) Name() string { return "ProductCreated" }

func main() {
	db, err := sql.Open("pgx", "postgres://user:password@localhost:5432/flux?sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	projStore := postgresproj.New(
		db,
		postgresproj.WithCheckpointsTable("checkpoints"),
	)

	consumerID := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:products")
	projector := projection.New(consumerID, eventStore, projStore)

	projection.RegisterHandler(projector, func(ctx projection.Context, ev ProductCreated) error {
		tx, ok := postgresproj.TxFromContext(ctx)
		if !ok {
			return fmt.Errorf("active transaction missing from context")
		}

		_, err := tx.ExecContext(ctx,
			`INSERT INTO catalog_products (id, name, price)
			 VALUES ($1, $2, $3)
			 ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, price = EXCLUDED.price`,
			ev.ProductID, ev.Name, ev.Price,
		)
		return err
	})
}
```

::: warning Same-Database Requirement
Atomicity is only achieved when the application SQL executed inside the handler operates through the transaction extracted via `postgresproj.TxFromContext(ctx)`. If the handler accesses a different database connection or skips `tx`, atomicity between the read model and checkpoint cursor is lost.
:::

### Direct `Update` (without `Projector`)

If your application updates a read model manually outside the background `Projector` engine, call `Store.Update` directly and retrieve the transaction from `txCtx` inside the `mutate` closure:

```go
err := projStore.Update(ctx, consumerID, env, func(txCtx context.Context) error {
	tx, ok := postgresproj.TxFromContext(txCtx)
	if !ok {
		return fmt.Errorf("active transaction missing from context")
	}

	_, err := tx.ExecContext(txCtx,
		`INSERT INTO catalog_products (id, name, price)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, price = EXCLUDED.price`,
		ev.ProductID, ev.Name, ev.Price,
	)
	return err
})
```


