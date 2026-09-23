package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/wotek/flux"
	snapstore "github.com/wotek/flux/snapshot/store"
)

type accountState struct {
	Balance int
	Owner   string
}

func TestSnapshotStore_SaveAndLoad(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		stream   flux.Stream
		snapshot flux.Snapshot[accountState]
	}{
		{
			name: "single stream initial snapshot",
			stream: flux.Stream{
				Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:account:acc-001"),
			},
			snapshot: flux.Snapshot[accountState]{
				State:    accountState{Balance: 100, Owner: "Alice"},
				Revision: 1,
			},
		},
		{
			name: "different stream snapshot",
			stream: flux.Stream{
				Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:account:acc-002"),
			},
			snapshot: flux.Snapshot[accountState]{
				State:    accountState{Balance: 250, Owner: "Bob"},
				Revision: 5,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := snapstore.New[accountState]()
			ctx := context.Background()

			if err := store.Save(ctx, tt.stream, tt.snapshot); err != nil {
				t.Fatalf("failed to save snapshot: %v", err)
			}

			loaded, err := store.Load(ctx, tt.stream)
			if err != nil {
				t.Fatalf("failed to load snapshot: %v", err)
			}

			if loaded.Revision != tt.snapshot.Revision {
				t.Errorf("expected revision %d, got %d", tt.snapshot.Revision, loaded.Revision)
			}
			if loaded.State != tt.snapshot.State {
				t.Errorf("expected state %+v, got %+v", tt.snapshot.State, loaded.State)
			}
		})
	}
}

func TestSnapshotStore_LoadNotFound(t *testing.T) {
	t.Parallel()

	store := snapstore.New[accountState]()
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:account:nonexistent"),
	}

	_, err := store.Load(ctx, stream)
	if err == nil {
		t.Fatalf("expected error for nonexistent snapshot, got nil")
	}

	if !errors.Is(err, flux.ErrSnapshotNotFound) {
		t.Fatalf("expected error wrapping flux.ErrSnapshotNotFound, got: %v", err)
	}
}

func TestSnapshotStore_SaveOverwrite(t *testing.T) {
	t.Parallel()

	store := snapstore.New[accountState]()
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:account:acc-overwrite"),
	}

	snap1 := flux.Snapshot[accountState]{
		State:    accountState{Balance: 100, Owner: "Alice"},
		Revision: 1,
	}
	if err := store.Save(ctx, stream, snap1); err != nil {
		t.Fatalf("failed to save initial snapshot: %v", err)
	}

	loaded1, err := store.Load(ctx, stream)
	if err != nil {
		t.Fatalf("failed to load initial snapshot: %v", err)
	}
	if loaded1.Revision != 1 || loaded1.State.Balance != 100 {
		t.Fatalf("unexpected loaded snapshot: %+v", loaded1)
	}

	snap2 := flux.Snapshot[accountState]{
		State:    accountState{Balance: 300, Owner: "Alice Updated"},
		Revision: 5,
	}
	if err := store.Save(ctx, stream, snap2); err != nil {
		t.Fatalf("failed to save overwritten snapshot: %v", err)
	}

	loaded2, err := store.Load(ctx, stream)
	if err != nil {
		t.Fatalf("failed to load overwritten snapshot: %v", err)
	}
	if loaded2.Revision != 5 || loaded2.State.Balance != 300 || loaded2.State.Owner != "Alice Updated" {
		t.Fatalf("unexpected overwritten snapshot: %+v", loaded2)
	}
}

func TestSnapshotStore_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	store := snapstore.New[accountState]()
	ctx := context.Background()

	const (
		numGoroutines = 50
		numIterations = 100
	)

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := range numGoroutines {
		go func(workerID int) {
			defer wg.Done()

			stream := flux.Stream{
				Identifier: flux.MustParseIdentifier(fmt.Sprintf("urn:acme:prod:sales:tenant-1:account:worker-%d", workerID%5)),
			}

			for j := range numIterations {
				snap := flux.Snapshot[accountState]{
					State:    accountState{Balance: workerID*1000 + j, Owner: "Worker"},
					Revision: uint64(j + 1),
				}

				if err := store.Save(ctx, stream, snap); err != nil {
					t.Errorf("worker %d save error at iteration %d: %v", workerID, j, err)
					return
				}

				loaded, err := store.Load(ctx, stream)
				if err != nil && !errors.Is(err, flux.ErrSnapshotNotFound) {
					t.Errorf("worker %d load error at iteration %d: %v", workerID, j, err)
					return
				}
				if err == nil && loaded.Revision == 0 {
					t.Errorf("worker %d loaded invalid revision 0", workerID)
					return
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestSnapshotStore_ContextCancellation(t *testing.T) {
	t.Parallel()

	store := snapstore.New[accountState]()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:account:acc-cancel"),
	}

	snap := flux.Snapshot[accountState]{
		State:    accountState{Balance: 50, Owner: "Cancelled"},
		Revision: 1,
	}

	if err := store.Save(ctx, stream, snap); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled on Save, got %v", err)
	}

	if _, err := store.Load(ctx, stream); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled on Load, got %v", err)
	}
}

func TestNewSnapshotStore_ConstructorAlias(t *testing.T) {
	t.Parallel()

	store := snapstore.NewSnapshotStore[accountState]()
	if store == nil {
		t.Fatal("NewSnapshotStore returned nil")
	}

	ctx := context.Background()
	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:account:acc-alias"),
	}

	snap := flux.Snapshot[accountState]{
		State:    accountState{Balance: 10, Owner: "Alias"},
		Revision: 1,
	}

	if err := store.Save(ctx, stream, snap); err != nil {
		t.Fatalf("failed to save snapshot: %v", err)
	}

	loaded, err := store.Load(ctx, stream)
	if err != nil {
		t.Fatalf("failed to load snapshot: %v", err)
	}

	if loaded.Revision != 1 || loaded.State.Balance != 10 {
		t.Errorf("unexpected loaded snapshot: %+v", loaded)
	}
}
