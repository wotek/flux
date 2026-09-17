package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/projection/store"
)

func TestInMemoryProjectionStore(t *testing.T) {
	s := store.New()
	ctx := context.Background()
	id := flux.NewIdentifierFromString("urn:test:prod:proj:1:test:test")

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
		Revision: 5,
		Position: 5,
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
