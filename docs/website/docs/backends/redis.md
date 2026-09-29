# Redis Backend

Redis provides high-performance, in-memory storage suitable for fast prototyping, ephemeral caching, and fast-path event and snapshot persistence.

`flux` includes native Redis drivers for both the Event Store and Snapshot Store:
- **Event Store:** `github.com/wotek/flux/event/store/redis`
- **Snapshot Store:** `github.com/wotek/flux/snapshot/store/redis`

## Event Store

The Redis Event Store uses native Redis Streams (`XADD` / `XREAD`) with atomic Lua script execution for append operations. It guarantees optimistic concurrency control using contiguous sequence counters.

> [!WARNING]
> **Deployment Constraint: Standalone Redis Only**
> The atomic append operation uses a Lua script that coordinates across multiple keys (`revision:{urn}`, `stream:{urn}`, `position:global`, and `stream:global`). Because these keys map to different Redis hash slots, Redis Cluster will reject multi-key EVAL operations with a `CROSSSLOT` error. The Redis Event Store requires a standalone Redis instance or single-node primary/replica deployment. It is not currently supported on Redis Cluster.

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

## Checkpoint Store

The Redis Checkpoint Store persists the last successfully processed global event-stream position for tailing consumers such as `projection.Projector` and `workflow.Orchestrator`.

- **Package:** `github.com/wotek/flux/checkpoint/store/redis`

### Key Schema and Cluster Compatibility

- Key format: `{prefix}checkpoint:{consumerURN}` (e.g. `myapp:checkpoint:urn:acme:prod:projector:1:worker:lists`).
- Value: Decimal string representing the global stream position.

::: tip Redis Cluster Compatible
Unlike the multi-key Redis Event Store append script, the Checkpoint Store uses a **single-key** Lua script for compare-and-set max operations (`KEYS[1]`). It operates without cross-slot hash tags and is fully compatible with Redis Cluster as well as standalone Redis deployments.
:::

### Monotonic Max Lua Script

Updates are executed atomically using Lua to ensure positions are strictly monotonic:

```lua
local current = redis.call('GET', KEYS[1])
if not current then
    redis.call('SET', KEYS[1], ARGV[1])
else
    local new_val = tostring(ARGV[1])
    local cur_val = tostring(current)
    local new_len = #new_val
    local cur_len = #cur_val
    if new_len > cur_len or (new_len == cur_len and new_val > cur_val) then
        redis.call('SET', KEYS[1], new_val)
    end
end
return 1
```

The script evaluates string length followed by lexicographical digit comparison on equal lengths. This guarantees monotonic ordering for arbitrary unsigned 64-bit integer (`uint64`) positions without floating-point precision loss beyond Lua's `2^53 - 1` limit. If an earlier position is submitted (such as during at-least-once message redelivery), the call succeeds as a no-op without regressing the cursor.

### Usage

```go
package main

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wotek/flux"
	checkpointredis "github.com/wotek/flux/checkpoint/store/redis"
)

func main() {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	checkpointStore := checkpointredis.New(
		client,
		checkpointredis.WithKeyPrefix("myapp:"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	consumerID := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:analytics")

	if err := checkpointStore.SetPosition(ctx, consumerID, 450); err != nil {
		panic(err)
	}

	position, err := checkpointStore.GetPosition(ctx, consumerID)
	if err != nil {
		panic(err)
	}
	_ = position
}
```

