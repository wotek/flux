# In-Memory Backend

`flux` provides blazing-fast, thread-safe in-memory implementations for both the Event Store and Snapshot Store. These are ideal for local prototyping, unit testing, and fast-path CI execution without external database dependencies.

The in-memory drivers are available in:
- **Event Store:** `github.com/wotek/flux/event/store`
- **Snapshot Store:** `github.com/wotek/flux/snapshot/store`

## Event Store

The in-memory Event Store provides thread-safe append and stream operations protected by a reader-writer lock (`sync.RWMutex`), enforcing optimistic concurrency control matching the behavior of durable backends.

```go
package main

import (
	"context"
	"time"

	"github.com/wotek/flux"
	eventstore "github.com/wotek/flux/event/store"
)

type AccountCreated struct {
	AccountID string
	Owner     string
}

func (e AccountCreated) EventName() string {
	return "AccountCreated"
}

func main() {
	store := eventstore.New()
	ctx := context.Background()

	stream := flux.Stream{
		ID:   "account-101",
		Type: "Account",
	}

	env := flux.Envelope{
		EventID:       "evt-001",
		EventType:     "AccountCreated",
		Event:         AccountCreated{AccountID: "account-101", Owner: "Alice"},
		Revision:      1,
		Timestamp:     time.Now().UTC(),
		CorrelationID: "corr-001",
		CausationID:   "caus-001",
	}

	if err := store.Append(ctx, stream, 0, []flux.Envelope{env}); err != nil {
		panic(err)
	}

	iter, err := store.Read(ctx, stream, 0)
	if err != nil {
		panic(err)
	}

	for env, err := range iter {
		if err != nil {
			panic(err)
		}
		_ = env
	}
}
```

## Snapshot Store

The in-memory Snapshot Store provides thread-safe point-in-time state caching using a synchronized map:

```go
package main

import (
	"context"
	"time"

	"github.com/wotek/flux"
	snapstore "github.com/wotek/flux/snapshot/store"
)

type AccountState struct {
	Balance int
	Owner   string
}

func main() {
	store := snapstore.New[AccountState]()
	ctx := context.Background()

	stream := flux.Stream{
		ID:   "account-101",
		Type: "Account",
	}

	snap := flux.Snapshot[AccountState]{
		State:    AccountState{Balance: 150, Owner: "Alice"},
		Revision: 10,
	}

	if err := store.Save(ctx, stream, snap); err != nil {
		panic(err)
	}

	loaded, err := store.Load(ctx, stream)
	if err != nil {
		panic(err)
	}
	_ = loaded
}
```

## Characteristics

- **Zero Dependencies:** Pure Go standard library without network sockets, serialization overhead, or external processes.
- **Thread Safety:** All read and write operations are synchronized with `sync.RWMutex`.
- **Ephemeral:** Data exists solely in application memory and is reset upon process termination.

