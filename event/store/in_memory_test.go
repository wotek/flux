package store_test

import (
	"context"
	"errors"
	"testing"
	"time"
	"github.com/oklog/ulid/v2"

	"github.com/wotek/flux"
	eventstore "github.com/wotek/flux/event/store"
)

type dummyEvent struct {
	Value string
}

func (d dummyEvent) Name() string { return "DummyEvent" }

func makeEnvelope(stream flux.Stream, rev uint64, val string) flux.Envelope {
	return flux.Envelope{
		Identifier: flux.NewIdentifier("", "", "stream", "", "event", ulid.Make().String(), ""),
		Stream:     stream,
		Revision:   rev,
		Event:      dummyEvent{Value: val},
		CreatedAt:  time.Now(),
	}
}

func TestEventStore_ReadWithFromRevision(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		fromRevision uint64
		wantCount    int
		wantRevs     []uint64
	}{
		{
			name:         "read from beginning (0)",
			fromRevision: 0,
			wantCount:    3,
			wantRevs:     []uint64{1, 2, 3},
		},
		{
			name:         "read from revision 1",
			fromRevision: 1,
			wantCount:    2,
			wantRevs:     []uint64{2, 3},
		},
		{
			name:         "read from revision 2",
			fromRevision: 2,
			wantCount:    1,
			wantRevs:     []uint64{3},
		},
		{
			name:         "read from revision 3 (latest)",
			fromRevision: 3,
			wantCount:    0,
			wantRevs:     nil,
		},
		{
			name:         "read beyond latest revision",
			fromRevision: 10,
			wantCount:    0,
			wantRevs:     nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := eventstore.New()
			ctx := context.Background()
			stream := flux.Stream{
				Identifier: flux.MustParseIdentifier("urn:test:prod:items:123:item:" + tt.name),
			}

			events := []flux.Envelope{
				makeEnvelope(stream, 1, "first"),
				makeEnvelope(stream, 2, "second"),
				makeEnvelope(stream, 3, "third"),
			}

			if err := store.Append(ctx, stream, 0, events); err != nil {
				t.Fatalf("failed to append events: %v", err)
			}

			iter, err := store.Read(ctx, stream, tt.fromRevision)
			if err != nil {
				t.Fatalf("failed to read stream: %v", err)
			}

			var gotRevs []uint64
			for env, iterErr := range iter {
				if iterErr != nil {
					t.Fatalf("unexpected error during iteration: %v", iterErr)
				}
				gotRevs = append(gotRevs, env.Revision)
			}

			if len(gotRevs) != tt.wantCount {
				t.Fatalf("expected %d events, got %d", tt.wantCount, len(gotRevs))
			}

			for i, rev := range tt.wantRevs {
				if gotRevs[i] != rev {
					t.Errorf("at index %d: expected revision %d, got %d", i, rev, gotRevs[i])
				}
			}
		})
	}
}

func TestEventStore_ConcurrencyError(t *testing.T) {
	t.Parallel()

	store := eventstore.New()
	ctx := context.Background()
	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:test:prod:items:123:item:concurrency"),
	}

	initialEvents := []flux.Envelope{
		makeEnvelope(stream, 1, "first"),
	}

	if err := store.Append(ctx, stream, 0, initialEvents); err != nil {
		t.Fatalf("failed to append initial event: %v", err)
	}

	// Attempt to append with wrong expected revision (0 instead of 1)
	conflictingEvents := []flux.Envelope{
		makeEnvelope(stream, 2, "second"),
	}

	err := store.Append(ctx, stream, 0, conflictingEvents)
	if err == nil {
		t.Fatalf("expected concurrency error, got nil")
	}
	if !errors.Is(err, flux.ErrConcurrency) {
		t.Fatalf("expected ErrConcurrency, got %v", err)
	}
}
