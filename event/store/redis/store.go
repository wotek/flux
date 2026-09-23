package redis

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/wotek/flux"
	"github.com/wotek/flux/codec"
)

//go:embed append.lua
var appendScriptSource string

var _ flux.EventStore = (*EventStore)(nil)

// EventStore is a Redis Streams-backed implementation of [flux.EventStore].
type EventStore struct {
	client       redis.UniversalClient
	serializer   codec.Serializer
	config       config
	appendScript *redis.Script
}

// New creates a new Redis [EventStore].
func New(client redis.UniversalClient, serializer codec.Serializer, opts ...Option) *EventStore {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	return &EventStore{
		client:       client,
		serializer:   serializer,
		config:       cfg,
		appendScript: redis.NewScript(appendScriptSource),
	}
}

// NewEventStore is an alias for [New] to maintain explicit constructor naming.
func NewEventStore(client redis.UniversalClient, serializer codec.Serializer, opts ...Option) *EventStore {
	return New(client, serializer, opts...)
}

// Append adds new events to a specific stream, enforcing optimistic concurrency.
func (s *EventStore) Append(ctx context.Context, stream flux.Stream, expectedRevision uint64, events []flux.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if len(events) == 0 {
		return nil
	}

	streamURN := stream.Identifier.String()
	args := make([]any, 0, 2+len(events))
	args = append(args, expectedRevision, len(events))

	for i, env := range events {
		if env.CreatedAt.IsZero() {
			env.CreatedAt = time.Now().UTC()
		}
		env.Stream = stream
		env.Revision = expectedRevision + uint64(i) + 1

		payload, err := s.serializer.Marshal(env)
		if err != nil {
			return fmt.Errorf("marshaling envelope: %w", err)
		}
		args = append(args, payload)
	}

	keys := []string{
		s.revisionKey(streamURN),
		s.streamKey(streamURN),
		s.globalPosKey(),
		s.globalStreamKey(),
	}

	err := s.appendScript.Run(ctx, s.client, keys, args...).Err()
	if err != nil {
		if strings.Contains(err.Error(), "ERR_CONCURRENCY") {
			return fmt.Errorf("%w: %s", flux.ErrConcurrency, err.Error())
		}
		return fmt.Errorf("appending to redis stream %q: %w", streamURN, err)
	}

	return nil
}

// Read retrieves events for a specific stream starting after the given fromRevision.
// Passing 0 reads the entire stream from the beginning.
func (s *EventStore) Read(ctx context.Context, stream flux.Stream, fromRevision uint64) (flux.StreamIterator, error) {
	streamURN := stream.Identifier.String()
	streamKey := s.streamKey(streamURN)

	return func(yield func(flux.Envelope, error) bool) {
		lastID := fmt.Sprintf("%d-0", fromRevision)
		batchSize := s.config.batchSize
		if batchSize <= 0 {
			batchSize = 100
		}

		for {
			if err := ctx.Err(); err != nil {
				yield(flux.Envelope{}, err)
				return
			}

			streams, err := s.client.XRead(ctx, &redis.XReadArgs{
				Streams: []string{streamKey, lastID},
				Count:   batchSize,
				Block:   -1,
			}).Result()

			if err != nil {
				if errors.Is(err, redis.Nil) {
					return
				}
				yield(flux.Envelope{}, fmt.Errorf("reading redis stream %q: %w", streamKey, err))
				return
			}

			if len(streams) == 0 || len(streams[0].Messages) == 0 {
				return
			}

			messages := streams[0].Messages
			for _, msg := range messages {
				lastID = msg.ID

				rev, parseErr := parseStreamID(msg.ID)
				if parseErr != nil {
					yield(flux.Envelope{}, fmt.Errorf("parsing message id %q: %w", msg.ID, parseErr))
					return
				}

				payload, extractErr := extractPayload(msg.Values)
				if extractErr != nil {
					yield(flux.Envelope{}, extractErr)
					return
				}

				env, unmarshalErr := s.serializer.Unmarshal(payload)
				if unmarshalErr != nil {
					yield(flux.Envelope{}, fmt.Errorf("unmarshaling envelope: %w", unmarshalErr))
					return
				}

				env.Revision = rev
				env.Stream = stream

				if !yield(env, nil) {
					return
				}
			}

			if int64(len(messages)) < batchSize {
				return
			}
		}
	}, nil
}

// Stream retrieves events from the global event log starting after the given position.
func (s *EventStore) Stream(ctx context.Context, position uint64) (flux.StreamIterator, error) {
	globalStreamKey := s.globalStreamKey()

	return func(yield func(flux.Envelope, error) bool) {
		lastID := fmt.Sprintf("%d-0", position)
		batchSize := s.config.batchSize
		if batchSize <= 0 {
			batchSize = 100
		}

		for {
			if err := ctx.Err(); err != nil {
				yield(flux.Envelope{}, err)
				return
			}

			streams, err := s.client.XRead(ctx, &redis.XReadArgs{
				Streams: []string{globalStreamKey, lastID},
				Count:   batchSize,
				Block:   -1,
			}).Result()

			if err != nil {
				if errors.Is(err, redis.Nil) {
					return
				}
				yield(flux.Envelope{}, fmt.Errorf("reading global redis stream: %w", err))
				return
			}

			if len(streams) == 0 || len(streams[0].Messages) == 0 {
				return
			}

			messages := streams[0].Messages
			for _, msg := range messages {
				lastID = msg.ID

				pos, parseErr := parseStreamID(msg.ID)
				if parseErr != nil {
					yield(flux.Envelope{}, fmt.Errorf("parsing message id %q: %w", msg.ID, parseErr))
					return
				}

				payload, extractErr := extractPayload(msg.Values)
				if extractErr != nil {
					yield(flux.Envelope{}, extractErr)
					return
				}

				env, unmarshalErr := s.serializer.Unmarshal(payload)
				if unmarshalErr != nil {
					yield(flux.Envelope{}, fmt.Errorf("unmarshaling envelope: %w", unmarshalErr))
					return
				}

				env.Position = pos

				if !yield(env, nil) {
					return
				}
			}

			if int64(len(messages)) < batchSize {
				return
			}
		}
	}, nil
}

func (s *EventStore) revisionKey(streamURN string) string {
	if s.config.keyPrefix != "" {
		return s.config.keyPrefix + ":revision:" + streamURN
	}
	return "revision:" + streamURN
}

func (s *EventStore) streamKey(streamURN string) string {
	if s.config.keyPrefix != "" {
		return s.config.keyPrefix + ":stream:" + streamURN
	}
	return "stream:" + streamURN
}

func (s *EventStore) globalPosKey() string {
	if s.config.keyPrefix != "" {
		return s.config.keyPrefix + ":position:global"
	}
	return "position:global"
}

func (s *EventStore) globalStreamKey() string {
	if s.config.keyPrefix != "" {
		return s.config.keyPrefix + ":stream:global"
	}
	return "stream:global"
}

func parseStreamID(id string) (uint64, error) {
	before, _, _ := strings.Cut(id, "-")
	val, err := strconv.ParseUint(before, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing stream id %q: %w", id, err)
	}
	return val, nil
}

func extractPayload(values map[string]any) ([]byte, error) {
	val, ok := values["data"]
	if !ok {
		val, ok = values["payload"]
		if !ok {
			return nil, fmt.Errorf("redis stream message missing 'data' or 'payload' field")
		}
	}

	switch v := val.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		return nil, fmt.Errorf("unexpected payload type %T in redis stream message", val)
	}
}
