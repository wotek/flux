package platform

import (
	"context"
	"fmt"
	"os"
	"reflect"

	"github.com/redis/go-redis/v9"
	"github.com/wotek/flux"
	jsoncodec "github.com/wotek/flux/codec/json"
	"github.com/wotek/flux/command"
	"github.com/wotek/flux/event"
	memstore "github.com/wotek/flux/event/store"
	redisstore "github.com/wotek/flux/event/store/redis"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/aggregates/order"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/commands"
	"github.com/wotek/flux/example/temporal-payment/internal/sales/events"
)

// EventTypes returns the registry of sales events for codec-backed stores.
func EventTypes() *event.Types {
	types := event.NewTypes()
	event.RegisterType[events.OrderPlaced](types)
	event.RegisterType[events.OrderPaid](types)
	event.RegisterType[events.OrderCancelled](types)
	return types
}

// OpenEventStore opens a shared EventStore.
// When EVENTSTORE_REDIS_ADDR is set (e.g. localhost:6379), uses Redis so worker and demo share state.
// Otherwise uses an in-memory store (suitable for unit tests in a single process).
func OpenEventStore(ctx context.Context) (flux.EventStore, func(), error) {
	addr := os.Getenv("EVENTSTORE_REDIS_ADDR")
	if addr == "" {
		return memstore.New(), func() {}, nil
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, nil, fmt.Errorf("redis event store at %s: %w", addr, err)
	}

	serializer := jsoncodec.New(EventTypes())
	// JSON codecs instantiate event pointers for unmarshaling; wrap so the domain
	// always sees value-typed events (same shape as in-memory append).
	es := valueEvents{inner: redisstore.New(client, serializer)}
	cleanup := func() { _ = client.Close() }
	return es, cleanup, nil
}

// NewSalesStack wires the sales aggregate repository and command bus against the store.
func NewSalesStack(es flux.EventStore) (*flux.AggregateRepository[*order.OrderAggregate, events.OrderEvent], *command.Bus) {
	repo := flux.NewAggregateRepository[*order.OrderAggregate, events.OrderEvent](es)
	cmdBus := command.New()
	commands.Register(cmdBus, repo)
	return repo, cmdBus
}

// Env returns an environment variable or a default.
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// valueEvents adapts a codec-backed store so Read/Find/Stream yield value-typed events.
type valueEvents struct {
	inner flux.EventStore
}

func (s valueEvents) Append(ctx context.Context, stream flux.Stream, expectedRevision uint64, events []flux.Envelope) error {
	return s.inner.Append(ctx, stream, expectedRevision, events)
}

func (s valueEvents) Read(ctx context.Context, stream flux.Stream, fromRevision uint64) (flux.StreamIterator, error) {
	iter, err := s.inner.Read(ctx, stream, fromRevision)
	if err != nil {
		return nil, err
	}
	return mapEnvelopes(iter), nil
}

func (s valueEvents) Stream(ctx context.Context, position uint64) (flux.StreamIterator, error) {
	iter, err := s.inner.Stream(ctx, position)
	if err != nil {
		return nil, err
	}
	return mapEnvelopes(iter), nil
}

func (s valueEvents) Find(ctx context.Context, stream flux.Stream, eventID flux.Identifier) (flux.Envelope, error) {
	env, err := s.inner.Find(ctx, stream, eventID)
	if err != nil {
		return flux.Envelope{}, err
	}
	env.Event = valueEvent(env.Event)
	return env, nil
}

func mapEnvelopes(iter flux.StreamIterator) flux.StreamIterator {
	return func(yield func(flux.Envelope, error) bool) {
		for env, err := range iter {
			if err != nil {
				yield(flux.Envelope{}, err)
				return
			}
			env.Event = valueEvent(env.Event)
			if !yield(env, nil) {
				return
			}
		}
	}
}

func valueEvent(e flux.Event) flux.Event {
	if e == nil {
		return nil
	}
	v := reflect.ValueOf(e)
	if v.Kind() == reflect.Pointer && !v.IsNil() {
		return v.Elem().Interface().(flux.Event)
	}
	return e
}
