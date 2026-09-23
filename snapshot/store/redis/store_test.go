package redis_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/wotek/flux"
	snapstore "github.com/wotek/flux/snapshot/store/redis"
)

type cartState struct {
	Items []string `json:"items"`
	Total int      `json:"total"`
}

func setupSnapshotStore[S any](t *testing.T, opts ...snapstore.Option) (*snapstore.SnapshotStore[S], *goredis.Client) {
	t.Helper()

	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{
		Addr: mr.Addr(),
	})
	t.Cleanup(func() {
		_ = client.Close()
	})

	store := snapstore.New[S](client, opts...)
	return store, client
}

func TestSnapshotStore_SaveAndLoad(t *testing.T) {
	t.Parallel()

	store, _ := setupSnapshotStore[cartState](t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:cart:c-101"),
	}

	snap := flux.Snapshot[cartState]{
		State: cartState{
			Items: []string{"apple", "banana"},
			Total: 15,
		},
		Revision: 42,
	}

	if err := store.Save(ctx, stream, snap); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	loaded, err := store.Load(ctx, stream)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if loaded.Revision != snap.Revision {
		t.Errorf("Revision = %d, want %d", loaded.Revision, snap.Revision)
	}
	if !reflect.DeepEqual(loaded.State, snap.State) {
		t.Errorf("State = %+v, want %+v", loaded.State, snap.State)
	}
}

func TestSnapshotStore_NotFound(t *testing.T) {
	t.Parallel()

	store, _ := setupSnapshotStore[cartState](t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:cart:non-existent"),
	}

	_, err := store.Load(ctx, stream)
	if err == nil {
		t.Fatalf("expected error for non-existent snapshot, got nil")
	}
	if !errors.Is(err, snapstore.ErrSnapshotNotFound) {
		t.Fatalf("expected error wrapping ErrSnapshotNotFound, got: %v", err)
	}
}

func TestSnapshotStore_Overwrite(t *testing.T) {
	t.Parallel()

	store, _ := setupSnapshotStore[cartState](t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:cart:c-101"),
	}

	snap1 := flux.Snapshot[cartState]{
		State:    cartState{Items: []string{"apple"}, Total: 5},
		Revision: 10,
	}
	snap2 := flux.Snapshot[cartState]{
		State:    cartState{Items: []string{"apple", "orange"}, Total: 12},
		Revision: 20,
	}

	if err := store.Save(ctx, stream, snap1); err != nil {
		t.Fatalf("Save snap1 failed: %v", err)
	}
	if err := store.Save(ctx, stream, snap2); err != nil {
		t.Fatalf("Save snap2 failed: %v", err)
	}

	loaded, err := store.Load(ctx, stream)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if loaded.Revision != 20 {
		t.Errorf("expected Revision 20, got %d", loaded.Revision)
	}
	if !reflect.DeepEqual(loaded.State, snap2.State) {
		t.Errorf("State = %+v, want %+v", loaded.State, snap2.State)
	}
}

func TestSnapshotStore_KeyPrefixOption(t *testing.T) {
	t.Parallel()

	store, client := setupSnapshotStore[cartState](t, snapstore.WithKeyPrefix("custom_prefix"))
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:cart:c-101"),
	}

	snap := flux.Snapshot[cartState]{
		State:    cartState{Items: []string{"item"}, Total: 1},
		Revision: 1,
	}

	if err := store.Save(ctx, stream, snap); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	keys, err := client.Keys(ctx, "custom_prefix:*").Result()
	if err != nil {
		t.Fatalf("Keys failed: %v", err)
	}

	if len(keys) == 0 {
		t.Errorf("expected keys matching 'custom_prefix:*', got none")
	}
}

func TestSnapshotStore_ContextCancellation(t *testing.T) {
	t.Parallel()

	store, _ := setupSnapshotStore[cartState](t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:cart:c-101"),
	}

	snap := flux.Snapshot[cartState]{
		State:    cartState{Items: []string{"item"}, Total: 1},
		Revision: 1,
	}

	if err := store.Save(ctx, stream, snap); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled on Save, got %v", err)
	}

	if _, err := store.Load(ctx, stream); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled on Load, got %v", err)
	}
}
