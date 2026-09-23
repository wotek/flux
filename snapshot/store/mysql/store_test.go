package mysql_test

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"reflect"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/wotek/flux"
	snapstoremysql "github.com/wotek/flux/snapshot/store/mysql"
)

//go:embed schema.sql
var schemaSQL string

type cartState struct {
	Items []string `json:"items"`
	Total int      `json:"total"`
}

func setupSnapshotStore[S any](t *testing.T, opts ...snapstoremysql.Option) (*snapstoremysql.SnapshotStore[S], sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock database: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	mock.ExpectExec(regexp.QuoteMeta(schemaSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if _, err := db.ExecContext(context.Background(), schemaSQL); err != nil {
		t.Fatalf("failed to initialize schema: %v", err)
	}

	store := snapstoremysql.New[S](db, opts...)
	return store, mock
}

func TestSchemaInitialization(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock database: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	ctx := context.Background()
	mock.ExpectExec(regexp.QuoteMeta(schemaSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		t.Fatalf("executing schema failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled sql expectations: %v", err)
	}
}

func TestSnapshotStore_SaveAndLoad(t *testing.T) {
	t.Parallel()

	store, mock := setupSnapshotStore[cartState](t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:cart:c-101"),
	}

	snap := flux.Snapshot[cartState]{
		State: cartState{
			Items: []string{"apple", "banana"},
			Total: 15,
		},
		Revision: 42,
	}

	stateBytes, err := json.Marshal(snap.State)
	if err != nil {
		t.Fatalf("Marshal state failed: %v", err)
	}

	// 1. Mock Save
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO snapshots")).
		WithArgs(
			stream.Identifier.String(),
			snap.Revision,
			stateBytes,
			sqlmock.AnyArg(), // timestamp
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := store.Save(ctx, stream, snap); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// 2. Mock Load
	mock.ExpectQuery(regexp.QuoteMeta("SELECT revision, snapshot FROM snapshots WHERE stream_id = ?")).
		WithArgs(stream.Identifier.String()).
		WillReturnRows(sqlmock.NewRows([]string{"revision", "snapshot"}).
			AddRow(snap.Revision, stateBytes))

	loaded, err := store.Load(ctx, stream)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if loaded.Revision != snap.Revision {
		t.Errorf("Revision = %d, want %d", loaded.Revision, snap.Revision)
	}
	if !reflect.DeepEqual(loaded.State, snap.State) {
		t.Errorf("State = %+v, want %+v", loaded.State, snap.State)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled sql expectations: %v", err)
	}
}

func TestSnapshotStore_NotFound(t *testing.T) {
	t.Parallel()

	store, mock := setupSnapshotStore[cartState](t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:cart:non-existent"),
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT revision, snapshot FROM snapshots WHERE stream_id = ?")).
		WithArgs(stream.Identifier.String()).
		WillReturnError(sql.ErrNoRows)

	_, err := store.Load(ctx, stream)
	if err == nil {
		t.Fatalf("expected error for non-existent snapshot, got nil")
	}
	if !errors.Is(err, flux.ErrSnapshotNotFound) {
		t.Fatalf("expected error wrapping ErrSnapshotNotFound, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled sql expectations: %v", err)
	}
}

func TestSnapshotStore_WithCustomTableName(t *testing.T) {
	t.Parallel()

	store, mock := setupSnapshotStore[cartState](t, snapstoremysql.WithTableName("custom_snapshots"))
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:cart:c-101"),
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT revision, snapshot FROM custom_snapshots WHERE stream_id = ?")).
		WithArgs("urn:acme:prod:sales:tenant-1:cart:c-101").
		WillReturnError(sql.ErrNoRows)

	_, err := store.Load(ctx, stream)
	if !errors.Is(err, flux.ErrSnapshotNotFound) {
		t.Fatalf("expected ErrSnapshotNotFound, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled sql expectations: %v", err)
	}
}

func TestSnapshotStore_ContextCancellation(t *testing.T) {
	t.Parallel()

	store, _ := setupSnapshotStore[cartState](t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:cart:c-101"),
	}

	snap := flux.Snapshot[cartState]{
		State:    cartState{Items: []string{"item"}, Total: 1},
		Revision: 1,
	}

	if err := store.Save(ctx, stream, snap); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled on Save, got %v", err)
	}

	if _, err := store.Load(ctx, stream); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled on Load, got %v", err)
	}
}
