# MySQL Backend

The MySQL backend provides ACID-compliant event storage and snapshot persistence using relational tables in MySQL or compatible engines (such as MariaDB).

`flux` includes native MySQL drivers for both the Event Store and Snapshot Store:
- **Event Store:** `github.com/wotek/flux/event/store/mysql`
- **Snapshot Store:** `github.com/wotek/flux/snapshot/store/mysql`

## Event Store

The MySQL Event Store persists events into a relational schema with decomposed columns for indexing, observability, and auditability. Appends are executed within a database transaction, guaranteeing atomic batch writes and strict optimistic concurrency control. Point-read retrieval of a single event envelope by stream and event identifier is provided via `Find(ctx, stream, eventID)`.

### Relational Schema

```sql
CREATE TABLE IF NOT EXISTS events (
    position BIGINT AUTO_INCREMENT PRIMARY KEY,
    stream_id VARCHAR(255) NOT NULL,
    stream_type VARCHAR(255) NULL,
    event_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    revision BIGINT UNSIGNED NOT NULL,
    event_data LONGBLOB NOT NULL,
    causation_id VARCHAR(255) NULL,
    correlation_id VARCHAR(255) NULL,
    timestamp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL,
    application_metadata JSON NULL,
    UNIQUE KEY uk_stream_revision (stream_id, revision),
    UNIQUE KEY uk_stream_event (stream_id, event_id),
    INDEX idx_stream_id (stream_id),
    INDEX idx_correlation_id (correlation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
```

Raw SQL schema definitions are provided alongside the package in `event/store/mysql/schema.sql` (and `snapshot/store/mysql/schema.sql`). Schema creation is decoupled from application runtime, allowing DBAs and migration tools (e.g., `golang-migrate` or `goose`) full control over database initialization and indexes. The `UNIQUE KEY uk_stream_event (stream_id, event_id)` constraint ensures that event IDs are unique within a stream and provides $O(1)$ index lookups for point-reads using `Find(ctx, stream, eventID)`.

### Optimistic Concurrency Control

Optimistic concurrency control is enforced through two complementary mechanisms:
1. **Row-level revision lock during append:** The store inspects `SELECT COALESCE(MAX(revision), 0) FROM events WHERE stream_id = ? FOR UPDATE` inside an active transaction. If the current stream revision does not match `expectedRevision`, the transaction aborts with `flux.ErrConcurrency`.
2. **Database unique constraint:** The `UNIQUE KEY uk_stream_revision (stream_id, revision)` ensures that even if concurrent transactions race, any duplicate revision insert fails with MySQL error code `1062`, which is unwrapped and returned as `flux.ErrConcurrency`.

### Codec Integration and Usage

The MySQL Event Store is decoupled from serialization formats by accepting any `codec.Serializer` (`codec/json`, `codec/xml`, or `codec/protobuf`):

```go
package main

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/wotek/flux"
	jsoncodec "github.com/wotek/flux/codec/json"
	"github.com/wotek/flux/event"
	mysqlstore "github.com/wotek/flux/event/store/mysql"
)

type AccountCreated struct {
	AccountID string `json:"account_id"`
	Owner     string `json:"owner"`
}

func (e AccountCreated) EventName() string {
	return "AccountCreated"
}

func main() {
	db, err := sql.Open("mysql", "user:password@tcp(127.0.0.1:3306)/flux?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	registry := event.NewTypes()
	event.RegisterType[AccountCreated](registry)
	serializer := jsoncodec.New(registry)

	store := mysqlstore.New(
		db,
		serializer,
		mysqlstore.WithTableName("events"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream := flux.Stream{
		ID:   "account-123",
		Type: "Account",
	}

	env := flux.Envelope{
		EventID:       "evt-001",
		EventType:     "AccountCreated",
		Event:         AccountCreated{AccountID: "account-123", Owner: "Alice"},
		Revision:      1,
		Timestamp:     time.Now().UTC(),
		CorrelationID: "corr-001",
		CausationID:   "caus-001",
		Metadata:      map[string]any{"source": "web"},
	}

	if err := store.Append(ctx, stream, 0, []flux.Envelope{env}); err != nil {
		panic(err)
	}
}
```

## Snapshot Store

The MySQL Snapshot Store provides state caching by persisting serialized aggregate state along with revision metadata:

### Snapshot Schema

```sql
CREATE TABLE IF NOT EXISTS snapshots (
    stream_id VARCHAR(255) PRIMARY KEY,
    revision BIGINT UNSIGNED NOT NULL,
    snapshot LONGBLOB NOT NULL,
    timestamp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
```

### Usage

```go
package main

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/wotek/flux"
	mysqlsnapshot "github.com/wotek/flux/snapshot/store/mysql"
)

type AccountState struct {
	Balance int `json:"balance"`
}

func main() {
	db, err := sql.Open("mysql", "user:password@tcp(127.0.0.1:3306)/flux?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	snapshotStore := mysqlsnapshot.New[AccountState](
		db,
		mysqlsnapshot.WithTableName("snapshots"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream := flux.Stream{
		ID:   "account-123",
		Type: "Account",
	}

	snap := flux.Snapshot[AccountState]{
		Revision:  10,
		State:     AccountState{Balance: 500},
		Timestamp: time.Now().UTC(),
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

The MySQL Checkpoint Store persists the last successfully processed global event-stream position for tailing consumers such as `projection.Projector` and `workflow.Orchestrator`.

- **Package:** `github.com/wotek/flux/checkpoint/store/mysql`

### Relational Schema

```sql
CREATE TABLE IF NOT EXISTS checkpoints (
    consumer_id VARCHAR(512) NOT NULL,
    position BIGINT UNSIGNED NOT NULL,
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (consumer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
```

The `consumer_id` column stores the full string representation of the consumer's `flux.Identifier` (e.g. `urn:acme:prod:projector:1:worker:lists`). The column width is set to `VARCHAR(512)` to safely accommodate environment-qualified URNs without truncation.

### Monotonic Max Semantics

Checkpoints are updated using an `INSERT ... ON DUPLICATE KEY UPDATE` query that enforces monotonic max semantics:

```sql
INSERT INTO checkpoints (consumer_id, position)
VALUES (?, ?)
ON DUPLICATE KEY UPDATE
    position = IF(VALUES(position) >= position, VALUES(position), position),
    updated_at = IF(VALUES(position) >= position, VALUES(updated_at), updated_at);
```

If an at-least-once retry or delayed worker attempts to commit an older position, MySQL leaves the existing higher position intact.

### Usage

```go
package main

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/wotek/flux"
	checkpointmysql "github.com/wotek/flux/checkpoint/store/mysql"
	"github.com/wotek/flux/workflow"
)

func main() {
	db, err := sql.Open("mysql", "user:password@tcp(127.0.0.1:3306)/flux?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	checkpointStore := checkpointmysql.New(
		db,
		checkpointmysql.WithTableName("checkpoints"),
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

The MySQL Projection Store runs read-model SQL mutations and consumer checkpoint position updates in a **single atomic database transaction**.

- **Package:** `github.com/wotek/flux/projection/store/mysql`

### Transactional Atomicity vs. Best-Effort

| Approach | Architecture | Atomicity Guarantee |
|----------|--------------|---------------------|
| **Same-Database MySQL Store** (`projection/store/mysql`) | Read-model tables and checkpoints table live in the **same** MySQL database instance | **ACID Transactional:** Mutation and checkpoint commit together in one transaction. If mutation fails, rollback prevents checkpoint advance. |
| **Cross-Database / Composed Store** | Read-model in PostgreSQL / Redis / Elasticsearch; checkpoint in MySQL / Redis | **Best-Effort:** Application mutates external read model, then calls `checkpoint.Store.SetPosition`. Crash between mutation and position save causes at-least-once redelivery. |

### Schema Requirement

The projection store uses the standard `checkpoints` table. Refer to [`checkpoint/store/mysql/schema.sql`](https://github.com/wotek/flux/blob/main/checkpoint/store/mysql/schema.sql) for the table DDL definition.

### Usage with `TxFromContext`

When `Projector` processes an envelope, `Store.Update` begins a MySQL transaction and injects it into the context passed to registered event handlers. Retrieve the transaction using `mysqlproj.TxFromContext(ctx)`:

```go
package main

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/wotek/flux"
	"github.com/wotek/flux/projection"
	mysqlproj "github.com/wotek/flux/projection/store/mysql"
)

type ProductCreated struct {
	ProductID string
	Name      string
	Price     int
}

func (e ProductCreated) Name() string { return "ProductCreated" }

func main() {
	db, err := sql.Open("mysql", "user:password@tcp(127.0.0.1:3306)/flux?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	projStore := mysqlproj.New(
		db,
		mysqlproj.WithCheckpointsTable("checkpoints"),
	)

	consumerID := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:products")
	projector := projection.New(consumerID, eventStore, projStore)

	projection.RegisterHandler(projector, func(ctx projection.Context, ev ProductCreated) error {
		tx, ok := mysqlproj.TxFromContext(ctx)
		if !ok {
			return fmt.Errorf("active transaction missing from context")
		}

		_, err := tx.ExecContext(ctx,
			"INSERT INTO catalog_products (id, name, price) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name), price = VALUES(price)",
			ev.ProductID, ev.Name, ev.Price,
		)
		return err
	})
}
```

::: warning Same-Database Requirement
Atomicity is only achieved when the application SQL executed inside the handler operates through the transaction extracted via `mysqlproj.TxFromContext(ctx)`. If the handler accesses a different database connection or skips `tx`, atomicity between the read model and checkpoint cursor is lost.
:::

### Direct `Update` (without `Projector`)

If your application updates a read model manually outside the background `Projector` engine, call `Store.Update` directly and retrieve the transaction from `txCtx` inside the `mutate` closure:

```go
err := projStore.Update(ctx, consumerID, env, func(txCtx context.Context) error {
	tx, ok := mysqlproj.TxFromContext(txCtx)
	if !ok {
		return fmt.Errorf("active transaction missing from context")
	}

	_, err := tx.ExecContext(txCtx,
		"INSERT INTO catalog_products (id, name, price) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name), price = VALUES(price)",
		ev.ProductID, ev.Name, ev.Price,
	)
	return err
})
```

