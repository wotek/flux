package store_test

import (
	"context"
	"sync"
	"testing"

	"github.com/wotek/flux"
	checkpointstore "github.com/wotek/flux/checkpoint/store"
)

func TestInMemoryStore_MissingReturnsZero(t *testing.T) {
	t.Parallel()

	s := checkpointstore.New()
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:consumer:1:worker:1")

	pos, err := s.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pos != 0 {
		t.Fatalf("expected position 0 for missing consumer, got %d", pos)
	}
}

func TestInMemoryStore_SetAndGet(t *testing.T) {
	t.Parallel()

	s := checkpointstore.NewStore()
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:consumer:1:worker:1")

	if err := s.SetPosition(ctx, id, 42); err != nil {
		t.Fatalf("unexpected SetPosition error: %v", err)
	}

	pos, err := s.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("unexpected GetPosition error: %v", err)
	}
	if pos != 42 {
		t.Fatalf("expected position 42, got %d", pos)
	}
}

func TestInMemoryStore_MonotonicMax(t *testing.T) {
	t.Parallel()

	s := checkpointstore.New()
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:consumer:1:worker:1")

	if err := s.SetPosition(ctx, id, 100); err != nil {
		t.Fatalf("unexpected SetPosition error: %v", err)
	}

	// Attempt to set an older position (e.g. at-least-once retry)
	if err := s.SetPosition(ctx, id, 50); err != nil {
		t.Fatalf("unexpected SetPosition error on older position: %v", err)
	}

	pos, err := s.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("unexpected GetPosition error: %v", err)
	}
	if pos != 100 {
		t.Fatalf("expected position to stay 100, got %d", pos)
	}

	// Setting equal position succeeds
	if err := s.SetPosition(ctx, id, 100); err != nil {
		t.Fatalf("unexpected SetPosition error on equal position: %v", err)
	}

	// Setting higher position updates
	if err := s.SetPosition(ctx, id, 150); err != nil {
		t.Fatalf("unexpected SetPosition error on higher position: %v", err)
	}

	pos, err = s.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("unexpected GetPosition error: %v", err)
	}
	if pos != 150 {
		t.Fatalf("expected position 150, got %d", pos)
	}
}

func TestInMemoryStore_ContextCancellation(t *testing.T) {
	t.Parallel()

	s := checkpointstore.New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	id := flux.MustParseIdentifier("urn:acme:prod:consumer:1:worker:1")

	if _, err := s.GetPosition(ctx, id); err == nil {
		t.Fatal("expected error on canceled context for GetPosition")
	}

	if err := s.SetPosition(ctx, id, 10); err == nil {
		t.Fatal("expected error on canceled context for SetPosition")
	}
}

func TestInMemoryStore_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	s := checkpointstore.New()
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:consumer:1:worker:concurrent")

	var wg sync.WaitGroup
	for i := uint64(1); i <= 50; i++ {
		wg.Add(2)
		pos := i
		go func() {
			defer wg.Done()
			_ = s.SetPosition(ctx, id, pos)
		}()
		go func() {
			defer wg.Done()
			_, _ = s.GetPosition(ctx, id)
		}()
	}
	wg.Wait()

	finalPos, err := s.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if finalPos == 0 {
		t.Fatal("expected non-zero final position")
	}
}
