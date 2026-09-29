package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wotek/flux"
	checkpointstore "github.com/wotek/flux/checkpoint/store"
	"github.com/wotek/flux/projection/store"
)

func TestInMemoryProjectionStore_Basic(t *testing.T) {
	t.Parallel()

	s := store.New()
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:test:prod:proj:1:test:test")

	// Initial position should be 0
	pos, err := s.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if pos != 0 {
		t.Fatalf("expected position 0, got %d", pos)
	}

	// Update position and test transaction
	env := flux.Envelope{
		Revision:  5,
		Position:  5,
		CreatedAt: time.Now(),
	}

	err = s.Update(ctx, id, env, func(txCtx context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Check new position
	pos, err = s.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if pos != 5 {
		t.Fatalf("expected position 5, got %d", pos)
	}
}

func TestInMemoryProjectionStore_MutateErrorDoesNotAdvance(t *testing.T) {
	t.Parallel()

	s := store.NewProjectionStore()
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:test:prod:proj:1:test:rollback")

	env1 := flux.Envelope{Position: 10}
	if err := s.Update(ctx, id, env1, func(txCtx context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("expected success on first update: %v", err)
	}

	pos, err := s.GetPosition(ctx, id)
	if err != nil || pos != 10 {
		t.Fatalf("expected position 10, got %d (err: %v)", pos, err)
	}

	// Now attempt an update with failing mutate
	mutateErr := errors.New("database connection failed")
	env2 := flux.Envelope{Position: 20}
	err = s.Update(ctx, id, env2, func(txCtx context.Context) error {
		return mutateErr
	})
	if !errors.Is(err, mutateErr) {
		t.Fatalf("expected mutate error, got %v", err)
	}

	// Position must remain 10
	pos, err = s.GetPosition(ctx, id)
	if err != nil || pos != 10 {
		t.Fatalf("expected position to stay 10 after mutate failure, got %d (err: %v)", pos, err)
	}
}

func TestInMemoryProjectionStore_MonotonicMax(t *testing.T) {
	t.Parallel()

	s := store.New()
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:test:prod:proj:1:test:monotonic")

	env1 := flux.Envelope{Position: 50}
	if err := s.Update(ctx, id, env1, func(txCtx context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("update failed: %v", err)
	}

	// Redelivery with older position should succeed but not regress position
	envOlder := flux.Envelope{Position: 30}
	if err := s.Update(ctx, id, envOlder, func(txCtx context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("older update failed: %v", err)
	}

	pos, err := s.GetPosition(ctx, id)
	if err != nil || pos != 50 {
		t.Fatalf("expected position to remain 50, got %d (err: %v)", pos, err)
	}
}

func TestInMemoryProjectionStore_WithCheckpointStore(t *testing.T) {
	t.Parallel()

	cs := checkpointstore.New()
	s := store.New(store.WithCheckpointStore(cs))
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:test:prod:proj:1:test:injected")

	// Pre-seed checkpoint
	if err := cs.SetPosition(ctx, id, 100); err != nil {
		t.Fatalf("failed to seed checkpoint: %v", err)
	}

	pos, err := s.GetPosition(ctx, id)
	if err != nil || pos != 100 {
		t.Fatalf("expected pre-seeded position 100, got %d (err: %v)", pos, err)
	}
}

func TestInMemoryProjectionStore_WithCheckpointStore_NilPanics(t *testing.T) {
	t.Parallel()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected WithCheckpointStore(nil) to panic")
		}
	}()

	_ = store.New(store.WithCheckpointStore(nil))
}
