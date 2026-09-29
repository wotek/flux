package redis_test

import (
	"context"
	"math"
	"testing"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/wotek/flux"
	checkpointredis "github.com/wotek/flux/checkpoint/store/redis"
)

func setupTestStore(t *testing.T, opts ...checkpointredis.Option) (*checkpointredis.Store, *goredis.Client) {
	t.Helper()

	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{
		Addr: mr.Addr(),
	})
	t.Cleanup(func() {
		_ = client.Close()
	})

	store := checkpointredis.New(client, opts...)
	return store, client
}

func TestGetPosition_MissingReturnsZero(t *testing.T) {
	t.Parallel()

	store, _ := setupTestStore(t)
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:w1")

	pos, err := store.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pos != 0 {
		t.Fatalf("expected position 0, got %d", pos)
	}
}

func TestSetPosition_Success(t *testing.T) {
	t.Parallel()

	store, _ := setupTestStore(t)
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:w2")

	if err := store.SetPosition(ctx, id, 42); err != nil {
		t.Fatalf("unexpected SetPosition error: %v", err)
	}

	pos, err := store.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("unexpected GetPosition error: %v", err)
	}
	if pos != 42 {
		t.Fatalf("expected position 42, got %d", pos)
	}
}

func TestSetPosition_MonotonicMax(t *testing.T) {
	t.Parallel()

	store, _ := setupTestStore(t)
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:w3")

	if err := store.SetPosition(ctx, id, 100); err != nil {
		t.Fatalf("SetPosition failed: %v", err)
	}

	// Attempt setting older position
	if err := store.SetPosition(ctx, id, 50); err != nil {
		t.Fatalf("SetPosition with older position failed: %v", err)
	}

	pos, err := store.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("GetPosition failed: %v", err)
	}
	if pos != 100 {
		t.Fatalf("expected position to stay 100, got %d", pos)
	}

	// Setting equal position
	if err := store.SetPosition(ctx, id, 100); err != nil {
		t.Fatalf("SetPosition with equal position failed: %v", err)
	}

	// Setting higher position updates
	if err := store.SetPosition(ctx, id, 200); err != nil {
		t.Fatalf("SetPosition with higher position failed: %v", err)
	}

	pos, err = store.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("GetPosition failed: %v", err)
	}
	if pos != 200 {
		t.Fatalf("expected position 200, got %d", pos)
	}
}

func TestWithKeyPrefix(t *testing.T) {
	t.Parallel()

	store, client := setupTestStore(t, checkpointredis.WithKeyPrefix("myapp:"))
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:w4")

	if err := store.SetPosition(ctx, id, 77); err != nil {
		t.Fatalf("SetPosition failed: %v", err)
	}

	expectedKey := "myapp:checkpoint:" + id.String()
	val, err := client.Get(ctx, expectedKey).Uint64()
	if err != nil {
		t.Fatalf("failed to fetch raw key %q: %v", expectedKey, err)
	}
	if val != 77 {
		t.Fatalf("expected raw value 77, got %d", val)
	}

	pos, err := store.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("GetPosition failed: %v", err)
	}
	if pos != 77 {
		t.Fatalf("expected position 77, got %d", pos)
	}
}

func TestContextCancelled(t *testing.T) {
	t.Parallel()

	store, _ := setupTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:w5")

	if _, err := store.GetPosition(ctx, id); err == nil {
		t.Fatal("expected error on cancelled context for GetPosition")
	}

	if err := store.SetPosition(ctx, id, 10); err == nil {
		t.Fatal("expected error on cancelled context for SetPosition")
	}
}

func TestConstructorAlias(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	s := checkpointredis.NewStore(client)
	if s == nil {
		t.Fatal("expected non-nil Store from NewStore")
	}
}

func TestSetPosition_LargeValuesBeyond2To53(t *testing.T) {
	t.Parallel()

	store, _ := setupTestStore(t)
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:wlarge")

	// 1<<53 is 9007199254740992, at the edge of IEEE 754 exact integer precision.
	// 1<<53 + 10 is 9007199254741002, strictly larger.
	base := uint64(1) << 53
	larger := base + 10
	evenLarger := uint64(math.MaxUint64)

	if err := store.SetPosition(ctx, id, base); err != nil {
		t.Fatalf("SetPosition with base 2^53 failed: %v", err)
	}

	pos, err := store.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("GetPosition failed: %v", err)
	}
	if pos != base {
		t.Fatalf("expected position %d, got %d", base, pos)
	}

	// Setting a smaller value near 2^53 (base - 1) must be ignored
	smaller := base - 1
	if err := store.SetPosition(ctx, id, smaller); err != nil {
		t.Fatalf("SetPosition with smaller position failed: %v", err)
	}

	pos, err = store.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("GetPosition failed: %v", err)
	}
	if pos != base {
		t.Fatalf("expected position to stay %d, got %d", base, pos)
	}

	// Setting larger (base + 10) must advance position
	if err := store.SetPosition(ctx, id, larger); err != nil {
		t.Fatalf("SetPosition with larger position failed: %v", err)
	}

	pos, err = store.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("GetPosition failed: %v", err)
	}
	if pos != larger {
		t.Fatalf("expected position %d, got %d", larger, pos)
	}

	// Setting MaxUint64 must advance position
	if err := store.SetPosition(ctx, id, evenLarger); err != nil {
		t.Fatalf("SetPosition with MaxUint64 failed: %v", err)
	}

	pos, err = store.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("GetPosition failed: %v", err)
	}
	if pos != evenLarger {
		t.Fatalf("expected position %d, got %d", evenLarger, pos)
	}

	// Setting larger (which is < MaxUint64) must not overwrite MaxUint64
	if err := store.SetPosition(ctx, id, larger); err != nil {
		t.Fatalf("SetPosition with smaller than MaxUint64 failed: %v", err)
	}

	pos, err = store.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("GetPosition failed: %v", err)
	}
	if pos != evenLarger {
		t.Fatalf("expected position to stay %d, got %d", evenLarger, pos)
	}
}
