//go:build integration

package mysql_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wotek/flux"
	checkpointmysql "github.com/wotek/flux/checkpoint/store/mysql"
	"github.com/wotek/flux/internal/sqllive"
)

var (
	liveCheckpointMySQLDB *sql.DB
	checkpointTableSeq    uint64
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, cleanup, err := sqllive.OpenMySQL(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start live mysql container: %v\n", err)
		os.Exit(1)
	}

	liveCheckpointMySQLDB = db
	code := m.Run()

	cleanup()
	os.Exit(code)
}

func uniqueCheckpointTableName(prefix string) string {
	id := atomic.AddUint64(&checkpointTableSeq, 1)
	return fmt.Sprintf("%s_%d", prefix, id)
}

func setupLiveCheckpointStore(t *testing.T, opts ...checkpointmysql.Option) (*checkpointmysql.Store, string) {
	t.Helper()

	tableName := uniqueCheckpointTableName("checkpoints")
	tableDDL := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    consumer_id VARCHAR(512) NOT NULL,
    position BIGINT UNSIGNED NOT NULL,
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (consumer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
`, tableName)

	sqllive.MustExecSchema(t, liveCheckpointMySQLDB, tableDDL)

	allOpts := append([]checkpointmysql.Option{checkpointmysql.WithTableName(tableName)}, opts...)
	store := checkpointmysql.New(liveCheckpointMySQLDB, allOpts...)

	return store, tableName
}

func TestLiveCheckpointStore_MissingReturnsZero(t *testing.T) {
	t.Parallel()

	store, _ := setupLiveCheckpointStore(t)
	ctx := context.Background()

	consumerID := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:missing-consumer")
	pos, err := store.GetPosition(ctx, consumerID)
	if err != nil {
		t.Fatalf("unexpected error getting missing position: %v", err)
	}
	if pos != 0 {
		t.Fatalf("expected 0, got %d", pos)
	}
}

func TestLiveCheckpointStore_MonotonicMax(t *testing.T) {
	t.Parallel()

	store, _ := setupLiveCheckpointStore(t)
	ctx := context.Background()

	consumerID := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:monotonic-worker")

	// Set initial position to 10
	if err := store.SetPosition(ctx, consumerID, 10); err != nil {
		t.Fatalf("SetPosition(10) failed: %v", err)
	}

	pos, err := store.GetPosition(ctx, consumerID)
	if err != nil {
		t.Fatalf("GetPosition failed: %v", err)
	}
	if pos != 10 {
		t.Fatalf("expected position 10, got %d", pos)
	}

	// At-least-once retry with older position (5) must not regress the position
	if err := store.SetPosition(ctx, consumerID, 5); err != nil {
		t.Fatalf("SetPosition(5) failed: %v", err)
	}

	pos, err = store.GetPosition(ctx, consumerID)
	if err != nil {
		t.Fatalf("GetPosition failed: %v", err)
	}
	if pos != 10 {
		t.Fatalf("expected position to remain 10 after older write, got %d", pos)
	}
}

func TestLiveCheckpointStore_Advance(t *testing.T) {
	t.Parallel()

	store, _ := setupLiveCheckpointStore(t)
	ctx := context.Background()

	consumerID := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:advance-worker")

	// Set position to 10
	if err := store.SetPosition(ctx, consumerID, 10); err != nil {
		t.Fatalf("SetPosition(10) failed: %v", err)
	}

	// Advance position to 20
	if err := store.SetPosition(ctx, consumerID, 20); err != nil {
		t.Fatalf("SetPosition(20) failed: %v", err)
	}

	pos, err := store.GetPosition(ctx, consumerID)
	if err != nil {
		t.Fatalf("GetPosition failed: %v", err)
	}
	if pos != 20 {
		t.Fatalf("expected advanced position 20, got %d", pos)
	}
}
