package projection_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wotek/flux"
	eventstore "github.com/wotek/flux/event/store"
	"github.com/wotek/flux/projection"
	projstore "github.com/wotek/flux/projection/store"
)

type AccountCreated struct {
	Owner string
}

func (e AccountCreated) Name() string { return "AccountCreated" }

type MoneyDeposited struct {
	Amount int
}

func (e MoneyDeposited) Name() string { return "MoneyDeposited" }

func TestProjector(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	eventStore := eventstore.New()
	projectionStore := projstore.New()

	projID := flux.MustParseIdentifier("urn:proj::::stats:1")
	projector := projection.New(projID, eventStore, projectionStore)

	var processedCount atomic.Int32
	projection.RegisterHandler(projector, func(ctx projection.Context, e AccountCreated) error {
		processedCount.Add(1)
		return nil
	})

	// Start projector in background
	go projector.Start(ctx)

	// Append some events
	streamID := flux.MustParseIdentifier("urn:bank::::acc:1")
	stream := flux.Stream{Identifier: streamID}

	err := eventStore.Append(ctx, stream, 0, []flux.Envelope{
		{Event: AccountCreated{Owner: "Bob"}},
		{Event: AccountCreated{Owner: "Alice"}},
		{Event: MoneyDeposited{Amount: 100}}, // Should be ignored
	})

	if err != nil {
		t.Fatalf("failed to append events: %v", err)
	}

	// Wait for projector to process
	time.Sleep(200 * time.Millisecond)

	if count := processedCount.Load(); count != 2 {
		t.Errorf("expected 2 AccountCreated events processed, got %d", count)
	}

	pos, _ := projectionStore.GetPosition(ctx, projID)
	if pos != 3 {
		t.Errorf("expected position 3, got %d", pos)
	}
}
