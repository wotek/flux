# Redis Backend

Redis provides high-performance, in-memory storage suitable for fast prototyping, ephemeral caching, and fast-path event and snapshot persistence.

`flux` includes native Redis drivers for both the Event Store and Snapshot Store:
- **Event Store:** `github.com/wotek/flux/event/store/redis`
- **Snapshot Store:** `github.com/wotek/flux/snapshot/store/redis`

## Event Store

The Redis Event Store uses native Redis Streams (`XADD` / `XREAD`) with atomic Lua script execution for append operations. It guarantees optimistic concurrency control using contiguous sequence counters.

### Key Schema

- `revision:{urn}` (String: tracks the aggregate's sequence revision)
- `stream:{urn}` (Stream: the aggregate's event log)
- `position:global` (String: tracks the global sequence position)
- `stream:global` (Stream: the global event log for tailing)

### Codec Integration

The Redis Event Store is decoupled from serialization formats by requiring a `codec.Serializer` (`codec/json`, `codec/xml`, or `codec/protobuf`):

```go
package main

import (
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/wotek/flux"
	jsoncodec "github.com/wotek/flux/codec/json"
	"github.com/wotek/flux/event"
	redisstore "github.com/wotek/flux/event/store/redis"
)

func main() {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	registry := event.NewTypes()
	serializer := jsoncodec.New(registry)

	eventStore := redisstore.New(
		client,
		serializer,
		redisstore.WithKeyPrefix("myapp"),
		redisstore.WithBatchSize(100),
	)

	_ = eventStore
}
```

## Snapshot Store

The Redis Snapshot Store provides point-in-time state caching using standard Redis `GET` / `SET` operations:

```go
package main

import (
	"github.com/redis/go-redis/v9"
	redissnapshot "github.com/wotek/flux/snapshot/store/redis"
)

type AccountState struct {
	Balance int `json:"balance"`
}

func main() {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	snapshotStore := redissnapshot.New[AccountState](
		client,
		redissnapshot.WithKeyPrefix("myapp"),
	)

	_ = snapshotStore
}
```

