package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"

	"github.com/wotek/flux"
	"github.com/wotek/flux/checkpoint"
)

var _ checkpoint.Store = (*Store)(nil)

// Lua script to update the checkpoint position atomically using monotonic max semantics.
// Uses string length and lexicographical comparison to safely handle arbitrary uint64 values
// beyond the 2^53-1 IEEE floating point limit of Lua's tonumber.
// This single-key script is fully compatible with Redis Cluster.
const setPositionLua = `
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
`

// Store is a Redis-backed implementation of [checkpoint.Store].
type Store struct {
	client redis.UniversalClient
	config config
}

// New creates a new Redis [Store].
func New(client redis.UniversalClient, opts ...Option) *Store {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	return &Store{
		client: client,
		config: cfg,
	}
}

// NewStore is an alias for [New] to maintain explicit constructor naming across packages.
func NewStore(client redis.UniversalClient, opts ...Option) *Store {
	return New(client, opts...)
}

func (s *Store) checkpointKey(consumerID string) string {
	return fmt.Sprintf("%scheckpoint:%s", s.config.keyPrefix, consumerID)
}

// GetPosition loads the last successfully processed global stream position
// for the consumer identified by id from Redis.
// Returns 0 and nil error if the consumer has no stored checkpoint.
func (s *Store) GetPosition(ctx context.Context, id flux.Identifier) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	key := s.checkpointKey(id.String())
	val, err := s.client.Get(ctx, key).Uint64()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, fmt.Errorf("getting checkpoint position from redis for %q: %w", id, err)
	}

	return val, nil
}

// SetPosition records that the consumer identified by id has successfully
// processed the global stream through position in Redis using monotonic max semantics.
// Older positions will not overwrite newer ones.
func (s *Store) SetPosition(ctx context.Context, id flux.Identifier, position uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	key := s.checkpointKey(id.String())
	err := s.client.Eval(ctx, setPositionLua, []string{key}, strconv.FormatUint(position, 10)).Err()
	if err != nil {
		return fmt.Errorf("saving checkpoint position to redis for %q: %w", id, err)
	}

	return nil
}
