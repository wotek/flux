//go:build integration

package mysql_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/internal/sqllive"
	snapstoremysql "github.com/wotek/flux/snapshot/store/mysql"
)

var (
	liveSnapMySQLDB *sql.DB
	snapTableSeq    uint64
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, cleanup, err := sqllive.OpenMySQL(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start live mysql container: %v\n", err)
		os.Exit(1)
	}

	liveSnapMySQLDB = db
	code := m.Run()

	cleanup()
	os.Exit(code)
}

func uniqueSnapTableName(prefix string) string {
	id := atomic.AddUint64(&snapTableSeq, 1)
	return fmt.Sprintf("%s_%d", prefix, id)
}

func setupLiveSnapshotStore[S any](t *testing.T) (*snapstoremysql.SnapshotStore[S], string) {
	t.Helper()

	tableName := uniqueSnapTableName("snapshots")
	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    stream_id VARCHAR(255) PRIMARY KEY,
    revision BIGINT UNSIGNED NOT NULL,
    snapshot LONGBLOB NOT NULL,
    timestamp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
);
`, tableName)

	sqllive.MustExecSchema(t, liveSnapMySQLDB, ddl)

	store := snapstoremysql.New[S](liveSnapMySQLDB, snapstoremysql.WithTableName(tableName))
	return store, tableName
}

func TestLiveSnapshotStore_SaveAndLoad(t *testing.T) {
	t.Parallel()

	store, _ := setupLiveSnapshotStore[cartState](t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:1:cart:c-101"),
	}

	snap := flux.Snapshot[cartState]{
		State: cartState{
			Items: []string{"apple", "banana"},
			Total: 15,
		},
		Revision: 42,
	}

	// 1. Save snapshot
	if err := store.Save(ctx, stream, snap); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// 2. Load snapshot back
	loaded, err := store.Load(ctx, stream)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if loaded.Revision != snap.Revision {
		t.Errorf("expected revision %d, got %d", snap.Revision, loaded.Revision)
	}
	if !reflect.DeepEqual(loaded.State, snap.State) {
		t.Errorf("expected state %+v, got %+v", snap.State, loaded.State)
	}
}

func TestLiveSnapshotStore_Monotonic(t *testing.T) {
	t.Parallel()

	store, _ := setupLiveSnapshotStore[cartState](t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:1:cart:c-102"),
	}

	snap10 := flux.Snapshot[cartState]{
		State:    cartState{Items: []string{"rev10-item"}, Total: 100},
		Revision: 10,
	}
	if err := store.Save(ctx, stream, snap10); err != nil {
		t.Fatalf("Save(snap10) failed: %v", err)
	}

	// Save older revision 5
	snap5 := flux.Snapshot[cartState]{
		State:    cartState{Items: []string{"rev5-older-item"}, Total: 50},
		Revision: 5,
	}
	if err := store.Save(ctx, stream, snap5); err != nil {
		t.Fatalf("Save(snap5) failed: %v", err)
	}

	// Must still have revision 10 and state from revision 10
	loaded, err := store.Load(ctx, stream)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if loaded.Revision != 10 {
		t.Fatalf("expected revision 10, got %d", loaded.Revision)
	}
	if !reflect.DeepEqual(loaded.State, snap10.State) {
		t.Fatalf("expected state from revision 10 %+v, got %+v", snap10.State, loaded.State)
	}
}

func TestLiveSnapshotStore_NotFound(t *testing.T) {
	t.Parallel()

	store, _ := setupLiveSnapshotStore[cartState](t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:1:cart:missing"),
	}

	_, err := store.Load(ctx, stream)
	if err == nil {
		t.Fatal("expected error for missing snapshot, got nil")
	}
	if !errors.Is(err, flux.ErrSnapshotNotFound) {
		t.Fatalf("expected ErrSnapshotNotFound, got: %v", err)
	}
}
