//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/internal/sqllive"
	postgresproj "github.com/wotek/flux/projection/store/postgres"
)

var (
	liveProjPGDB *sql.DB
	projTableSeq uint64
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, cleanup, err := sqllive.OpenPostgres(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start live postgres container: %v\n", err)
		os.Exit(1)
	}

	liveProjPGDB = db
	code := m.Run()

	cleanup()
	os.Exit(code)
}

func uniqueProjTableName(prefix string) string {
	id := atomic.AddUint64(&projTableSeq, 1)
	return fmt.Sprintf("%s_%d", prefix, id)
}

type liveProjFixture struct {
	store          *postgresproj.Store
	readModelTable string
}

func setupLiveProjectionStore(t *testing.T) liveProjFixture {
	t.Helper()

	checkpointsTable := uniqueProjTableName("checkpoints")
	readModelTable := uniqueProjTableName("proj_items")

	// DDL for checkpoints and custom read-model
	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    consumer_id TEXT NOT NULL,
    position BIGINT NOT NULL,
    updated_at TIMESTAMPTZ(6) NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_id)
);

CREATE TABLE IF NOT EXISTS %s (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    amount INT NOT NULL
);
`, checkpointsTable, readModelTable)

	sqllive.MustExecSchema(t, liveProjPGDB, ddl)

	store := postgresproj.New(liveProjPGDB, postgresproj.WithCheckpointsTable(checkpointsTable))
	return liveProjFixture{
		store:          store,
		readModelTable: readModelTable,
	}
}

func TestLiveProjectionStore_UpdateSuccess(t *testing.T) {
	t.Parallel()

	f := setupLiveProjectionStore(t)
	ctx := context.Background()

	consumerID := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:w1")
	env := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:1:event:e1"),
		Position:   42,
	}

	err := f.store.Update(ctx, consumerID, env, func(txCtx context.Context) error {
		tx, ok := postgresproj.TxFromContext(txCtx)
		if !ok {
			return errors.New("missing tx from context")
		}

		query := fmt.Sprintf("INSERT INTO %s (id, name, amount) VALUES ($1, $2, $3)", f.readModelTable)
		_, execErr := tx.ExecContext(txCtx, query, "item-1", "widget", 100)
		return execErr
	})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// 1. Verify checkpoint advanced to env.Position
	pos, err := f.store.GetPosition(ctx, consumerID)
	if err != nil {
		t.Fatalf("GetPosition failed: %v", err)
	}
	if pos != 42 {
		t.Fatalf("expected position 42, got %d", pos)
	}

	// 2. Verify read model committed
	var amount int
	checkQuery := fmt.Sprintf("SELECT amount FROM %s WHERE id = $1", f.readModelTable)
	if err := liveProjPGDB.QueryRowContext(ctx, checkQuery, "item-1").Scan(&amount); err != nil {
		t.Fatalf("reading read model failed: %v", err)
	}
	if amount != 100 {
		t.Fatalf("expected amount 100, got %d", amount)
	}
}

func TestLiveProjectionStore_MutateFails_RollsBack(t *testing.T) {
	t.Parallel()

	f := setupLiveProjectionStore(t)
	ctx := context.Background()

	consumerID := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:w2")
	env := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:1:event:e2"),
		Position:   50,
	}

	mutateErr := errors.New("business mutation failure")
	err := f.store.Update(ctx, consumerID, env, func(txCtx context.Context) error {
		tx, ok := postgresproj.TxFromContext(txCtx)
		if !ok {
			return errors.New("missing tx from context")
		}

		// Insert should be rolled back
		query := fmt.Sprintf("INSERT INTO %s (id, name, amount) VALUES ($1, $2, $3)", f.readModelTable)
		if _, execErr := tx.ExecContext(txCtx, query, "item-rollback", "gadget", 200); execErr != nil {
			return execErr
		}

		return mutateErr
	})
	if err == nil {
		t.Fatal("expected error from Update, got nil")
	}
	if !errors.Is(err, mutateErr) {
		t.Fatalf("expected mutateErr, got %v", err)
	}

	// 1. Checkpoint must NOT advance (remains 0)
	pos, err := f.store.GetPosition(ctx, consumerID)
	if err != nil {
		t.Fatalf("GetPosition failed: %v", err)
	}
	if pos != 0 {
		t.Fatalf("expected position 0, got %d", pos)
	}

	// 2. Read model row must be absent
	var count int
	checkQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id = $1", f.readModelTable)
	if err := liveProjPGDB.QueryRowContext(ctx, checkQuery, "item-rollback").Scan(&count); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows after rollback, got %d", count)
	}
}

func TestLiveProjectionStore_MonotonicCheckpoint(t *testing.T) {
	t.Parallel()

	f := setupLiveProjectionStore(t)
	ctx := context.Background()

	consumerID := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:w3")

	// 1. First Update at position 10
	env1 := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:1:event:e10"),
		Position:   10,
	}
	err := f.store.Update(ctx, consumerID, env1, func(txCtx context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("first Update failed: %v", err)
	}

	pos, err := f.store.GetPosition(ctx, consumerID)
	if err != nil {
		t.Fatalf("GetPosition failed: %v", err)
	}
	if pos != 10 {
		t.Fatalf("expected position 10, got %d", pos)
	}

	// 2. Second Update with older position 5 must not regress the checkpoint cursor
	env2 := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:1:event:e5"),
		Position:   5,
	}
	err = f.store.Update(ctx, consumerID, env2, func(txCtx context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("second Update failed: %v", err)
	}

	pos, err = f.store.GetPosition(ctx, consumerID)
	if err != nil {
		t.Fatalf("GetPosition failed: %v", err)
	}
	if pos != 10 {
		t.Fatalf("expected position to remain 10, got %d", pos)
	}
}
