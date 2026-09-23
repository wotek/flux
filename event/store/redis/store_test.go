package redis_test

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/wotek/flux"
	jsoncodec "github.com/wotek/flux/codec/json"
	xmlcodec "github.com/wotek/flux/codec/xml"
	"github.com/wotek/flux/event"
	redisstore "github.com/wotek/flux/event/store/redis"
)

type itemAdded struct {
	ItemName string `json:"item_name"`
	Count    int    `json:"count"`
}

func (itemAdded) Name() string {
	return "ItemAdded"
}

func setupTestStore(t *testing.T, opts ...redisstore.Option) (*redisstore.EventStore, *goredis.Client) {
	t.Helper()

	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{
		Addr: mr.Addr(),
	})
	t.Cleanup(func() {
		_ = client.Close()
	})

	registry := event.NewTypes()
	event.RegisterType[itemAdded](registry)
	serializer := jsoncodec.New(registry)

	store := redisstore.New(client, serializer, opts...)
	return store, client
}

func TestEventStore_AppendAndRead(t *testing.T) {
	t.Parallel()

	store, _ := setupTestStore(t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:product:prod-1"),
	}

	env1 := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:event:evt-1"),
		Event: &itemAdded{
			ItemName: "Keyboard",
			Count:    1,
		},
		Actor: flux.Actor{
			Identifier: flux.MustParseIdentifier("urn:acme:prod:iam:tenant-1:user:usr-1"),
		},
	}

	env2 := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:event:evt-2"),
		Event: &itemAdded{
			ItemName: "Mouse",
			Count:    2,
		},
	}

	// 1. Initial append with expected revision 0
	if err := store.Append(ctx, stream, 0, []flux.Envelope{env1, env2}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// 2. Read all events from beginning (fromRevision 0)
	iter, err := store.Read(ctx, stream, 0)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	var readEvents []flux.Envelope
	for env, iterErr := range iter {
		if iterErr != nil {
			t.Fatalf("iteration error: %v", iterErr)
		}
		readEvents = append(readEvents, env)
	}

	if len(readEvents) != 2 {
		t.Fatalf("expected 2 events, got %d", len(readEvents))
	}

	if readEvents[0].Revision != 1 || readEvents[1].Revision != 2 {
		t.Errorf("unexpected revisions: %d, %d", readEvents[0].Revision, readEvents[1].Revision)
	}

	if readEvents[0].Stream != stream || readEvents[1].Stream != stream {
		t.Errorf("unexpected stream on events")
	}

	// 3. Read starting from revision 1 (should only return env2)
	iter2, err := store.Read(ctx, stream, 1)
	if err != nil {
		t.Fatalf("Read(fromRevision 1) failed: %v", err)
	}

	var partialEvents []flux.Envelope
	for env, iterErr := range iter2 {
		if iterErr != nil {
			t.Fatalf("iteration error: %v", iterErr)
		}
		partialEvents = append(partialEvents, env)
	}

	if len(partialEvents) != 1 {
		t.Fatalf("expected 1 event, got %d", len(partialEvents))
	}
	if partialEvents[0].Revision != 2 {
		t.Errorf("expected revision 2, got %d", partialEvents[0].Revision)
	}
}

func TestEventStore_OptimisticConcurrency(t *testing.T) {
	t.Parallel()

	store, _ := setupTestStore(t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:product:prod-1"),
	}

	env := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:event:evt-1"),
		Event: &itemAdded{
			ItemName: "Keyboard",
			Count:    1,
		},
	}

	// First append with revision 0 should succeed
	if err := store.Append(ctx, stream, 0, []flux.Envelope{env}); err != nil {
		t.Fatalf("first Append failed: %v", err)
	}

	// Second append with wrong expected revision 0 (should be 1) must return ErrConcurrency
	envConflict := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:event:evt-2"),
		Event: &itemAdded{
			ItemName: "Conflict",
			Count:    1,
		},
	}

	err := store.Append(ctx, stream, 0, []flux.Envelope{envConflict})
	if err == nil {
		t.Fatalf("expected concurrency error, got nil")
	}
	if !errors.Is(err, flux.ErrConcurrency) {
		t.Fatalf("expected error wrapping flux.ErrConcurrency, got: %v", err)
	}

	// Correct expected revision 1 should succeed
	if err := store.Append(ctx, stream, 1, []flux.Envelope{envConflict}); err != nil {
		t.Fatalf("subsequent Append with correct revision failed: %v", err)
	}
}

func TestEventStore_StreamGlobal(t *testing.T) {
	t.Parallel()

	store, _ := setupTestStore(t)
	ctx := context.Background()

	streamA := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:product:prod-a"),
	}
	streamB := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:product:prod-b"),
	}

	envA := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:event:evt-a"),
		Event:      &itemAdded{ItemName: "A", Count: 1},
	}
	envB := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:event:evt-b"),
		Event:      &itemAdded{ItemName: "B", Count: 2},
	}

	if err := store.Append(ctx, streamA, 0, []flux.Envelope{envA}); err != nil {
		t.Fatalf("Append A failed: %v", err)
	}
	if err := store.Append(ctx, streamB, 0, []flux.Envelope{envB}); err != nil {
		t.Fatalf("Append B failed: %v", err)
	}

	// Stream global from position 0
	iter, err := store.Stream(ctx, 0)
	if err != nil {
		t.Fatalf("Stream failed: %v", err)
	}

	var globalEvents []flux.Envelope
	for env, iterErr := range iter {
		if iterErr != nil {
			t.Fatalf("iteration error: %v", iterErr)
		}
		globalEvents = append(globalEvents, env)
	}

	if len(globalEvents) != 2 {
		t.Fatalf("expected 2 global events, got %d", len(globalEvents))
	}
	if globalEvents[0].Position != 1 || globalEvents[1].Position != 2 {
		t.Errorf("unexpected positions: %d, %d", globalEvents[0].Position, globalEvents[1].Position)
	}

	// Stream global from position 1
	iter2, err := store.Stream(ctx, 1)
	if err != nil {
		t.Fatalf("Stream(from 1) failed: %v", err)
	}

	var partialGlobal []flux.Envelope
	for env, iterErr := range iter2 {
		if iterErr != nil {
			t.Fatalf("iteration error: %v", iterErr)
		}
		partialGlobal = append(partialGlobal, env)
	}

	if len(partialGlobal) != 1 {
		t.Fatalf("expected 1 global event, got %d", len(partialGlobal))
	}
	if partialGlobal[0].Position != 2 {
		t.Errorf("expected position 2, got %d", partialGlobal[0].Position)
	}
}

func TestEventStore_EmptyStream(t *testing.T) {
	t.Parallel()

	store, _ := setupTestStore(t)
	ctx := context.Background()

	nonExistentStream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:product:none"),
	}

	iter, err := store.Read(ctx, nonExistentStream, 0)
	if err != nil {
		t.Fatalf("Read on empty stream failed: %v", err)
	}

	count := 0
	for _, iterErr := range iter {
		if iterErr != nil {
			t.Fatalf("unexpected iteration error: %v", iterErr)
		}
		count++
	}

	if count != 0 {
		t.Errorf("expected 0 events, got %d", count)
	}
}

func TestEventStore_KeyPrefixOption(t *testing.T) {
	t.Parallel()

	store, client := setupTestStore(t, redisstore.WithKeyPrefix("testns"))
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:product:prod-1"),
	}

	env := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:event:evt-1"),
		Event:      &itemAdded{ItemName: "Item", Count: 1},
	}

	if err := store.Append(ctx, stream, 0, []flux.Envelope{env}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Verify keys in Redis start with "testns:"
	keys, err := client.Keys(ctx, "testns:*").Result()
	if err != nil {
		t.Fatalf("Keys failed: %v", err)
	}

	if len(keys) == 0 {
		t.Errorf("expected keys matching 'testns:*', got none")
	}
}

func TestEventStore_EmptyAppend(t *testing.T) {
	t.Parallel()

	store, _ := setupTestStore(t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:product:prod-1"),
	}

	if err := store.Append(ctx, stream, 0, nil); err != nil {
		t.Fatalf("Append(nil) failed: %v", err)
	}

	if err := store.Append(ctx, stream, 0, []flux.Envelope{}); err != nil {
		t.Fatalf("Append([]) failed: %v", err)
	}
}

func TestEventStore_ContextCancellation(t *testing.T) {
	t.Parallel()

	store, _ := setupTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:product:prod-1"),
	}

	env := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:event:evt-1"),
		Event:      &itemAdded{ItemName: "Item", Count: 1},
	}

	if err := store.Append(ctx, stream, 0, []flux.Envelope{env}); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}

	iter, err := store.Read(ctx, stream, 0)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	for _, iterErr := range iter {
		if !errors.Is(iterErr, context.Canceled) {
			t.Errorf("expected iteration error context.Canceled, got %v", iterErr)
		}
	}
}

func TestEventStore_WithXMLCodec(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	registry := event.NewTypes()
	event.RegisterType[itemAdded](registry)
	serializer := xmlcodec.New(registry)

	store := redisstore.New(client, serializer)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:product:prod-xml"),
	}
	env := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:catalog:tenant-1:event:evt-xml-1"),
		Event:      &itemAdded{ItemName: "Widget", Count: 5},
	}

	if err := store.Append(ctx, stream, 0, []flux.Envelope{env}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	iter, err := store.Read(ctx, stream, 0)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	var events []flux.Envelope
	for e, iterErr := range iter {
		if iterErr != nil {
			t.Fatalf("iteration error: %v", iterErr)
		}
		events = append(events, e)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	gotEvent, ok := events[0].Event.(*itemAdded)
	if !ok || gotEvent.ItemName != "Widget" || gotEvent.Count != 5 {
		t.Errorf("unexpected event payload: %+v", events[0].Event)
	}
}


