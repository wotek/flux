# MySQL Backend

The MySQL backend provides ACID-compliant event storage and snapshot persistence using relational tables in MySQL or compatible engines (such as MariaDB).

`flux` includes native MySQL drivers for both the Event Store and Snapshot Store:
- **Event Store:** `github.com/wotek/flux/event/store/mysql`
- **Snapshot Store:** `github.com/wotek/flux/snapshot/store/mysql`

## Event Store

The MySQL Event Store persists events into a relational schema with decomposed columns for indexing, observability, and auditability. Appends are executed within a database transaction, guaranteeing atomic batch writes and strict optimistic concurrency control.

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
    INDEX idx_stream_id (stream_id),
    INDEX idx_correlation_id (correlation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
```

Raw SQL schema definitions are provided alongside the package in `event/store/mysql/schema.sql` (and `snapshot/store/mysql/schema.sql`). Schema creation is decoupled from application runtime, allowing DBAs and migration tools (e.g., `golang-migrate` or `goose`) full control over database initialization and indexes.

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
