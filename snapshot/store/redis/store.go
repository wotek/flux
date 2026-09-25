package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/wotek/flux"
)

var _ flux.SnapshotStore[any] = (*SnapshotStore[any])(nil)

type snapshotDTO[S any] struct {
	State    S      `json:"state"`
	Revision uint64 `json:"revision"`
}

// SnapshotStore is a Redis-backed implementation of [flux.SnapshotStore].
type SnapshotStore[S any] struct {
	client redis.UniversalClient
	config config
}

// New creates a new Redis [SnapshotStore] for state type S.
func New[S any](client redis.UniversalClient, opts ...Option) *SnapshotStore[S] {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	return &SnapshotStore[S]{
		client: client,
		config: cfg,
	}
}

// NewSnapshotStore is an alias for [New] to maintain explicit constructor naming.
func NewSnapshotStore[S any](client redis.UniversalClient, opts ...Option) *SnapshotStore[S] {
	return New[S](client, opts...)
}

// Load retrieves the latest snapshot for the specified stream from Redis.
// Returns [flux.ErrSnapshotNotFound] if no snapshot is found.
func (s *SnapshotStore[S]) Load(ctx context.Context, stream flux.Stream) (flux.Snapshot[S], error) {
	if err := ctx.Err(); err != nil {
		return flux.Snapshot[S]{}, err
	}

	key := s.snapshotKey(stream.Identifier.String())
	data, err := s.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return flux.Snapshot[S]{}, fmt.Errorf("loading snapshot for %q: %w", stream.Identifier, flux.ErrSnapshotNotFound)
		}
		return flux.Snapshot[S]{}, fmt.Errorf("getting snapshot from redis for %q: %w", stream.Identifier, err)
	}

	var dto snapshotDTO[S]
	if err := json.Unmarshal(data, &dto); err != nil {
		return flux.Snapshot[S]{}, fmt.Errorf("unmarshaling snapshot for %q: %w", stream.Identifier, err)
	}

	return flux.Snapshot[S]{
		State:    dto.State,
		Revision: dto.Revision,
	}, nil
}

var saveSnapshotScript = redis.NewScript(`
local key = KEYS[1]
local new_rev = tonumber(ARGV[1])
local payload = ARGV[2]

local existing = redis.call('GET', key)
if existing then
    local ok, decoded = pcall(cjson.decode, existing)
    if ok and decoded and decoded.revision and tonumber(decoded.revision) > new_rev then
        return 'OK'
    end
end

redis.call('SET', key, payload)
return 'OK'
`)

// Save persists a snapshot for the specified stream in Redis.
// If an existing snapshot exists with a higher revision, the older snapshot is ignored
// to prevent out-of-order writes from regressing state.
func (s *SnapshotStore[S]) Save(ctx context.Context, stream flux.Stream, snap flux.Snapshot[S]) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	dto := snapshotDTO[S]{
		State:    snap.State,
		Revision: snap.Revision,
	}

	data, err := json.Marshal(dto)
	if err != nil {
		return fmt.Errorf("marshaling snapshot for %q: %w", stream.Identifier, err)
	}

	key := s.snapshotKey(stream.Identifier.String())
	if err := saveSnapshotScript.Run(ctx, s.client, []string{key}, snap.Revision, data).Err(); err != nil {
		return fmt.Errorf("saving snapshot to redis for %q: %w", stream.Identifier, err)
	}

	return nil
}

func (s *SnapshotStore[S]) snapshotKey(streamURN string) string {
	if s.config.keyPrefix != "" {
		return s.config.keyPrefix + ":snapshot:" + streamURN
	}
	return "snapshot:" + streamURN
}
