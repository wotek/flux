package postgres_test

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
	snapstorepg "github.com/wotek/flux/snapshot/store/postgres"
)

//go:embed schema.sql
var schemaSQL string

type cartState struct {
	Items []string `json:"items"`
	Total int      `json:"total"`
}

func setupSnapshotStore[S any](t *testing.T, opts ...snapstorepg.Option) (*snapstorepg.SnapshotStore[S], sqlmock.Sqlmock) {
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

	store := snapstorepg.New[S](db, opts...)
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
	expectedSaveQuery := `INSERT INTO snapshots AS s (stream_id, revision, snapshot, timestamp)
VALUES ($1, $2, $3, $4)
ON CONFLICT (stream_id) DO UPDATE SET
  revision = EXCLUDED.revision,
  snapshot = EXCLUDED.snapshot,
  timestamp = EXCLUDED.timestamp
WHERE EXCLUDED.revision >= s.revision`

	mock.ExpectExec(regexp.QuoteMeta(expectedSaveQuery)).
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
	expectedLoadQuery := `SELECT revision, snapshot FROM snapshots WHERE stream_id = $1`
	mock.ExpectQuery(regexp.QuoteMeta(expectedLoadQuery)).
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

func TestSnapshotStore_Save_Error(t *testing.T) {
	t.Parallel()

	store, mock := setupSnapshotStore[cartState](t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:cart:c-102"),
	}

	snap := flux.Snapshot[cartState]{
		State:    cartState{Items: []string{"apple"}, Total: 10},
		Revision: 1,
	}

	stateBytes, err := json.Marshal(snap.State)
	if err != nil {
		t.Fatalf("Marshal state failed: %v", err)
	}

	dbErr := errors.New("connection reset")
	expectedSaveQuery := `INSERT INTO snapshots AS s (stream_id, revision, snapshot, timestamp)
VALUES ($1, $2, $3, $4)
ON CONFLICT (stream_id) DO UPDATE SET
  revision = EXCLUDED.revision,
  snapshot = EXCLUDED.snapshot,
  timestamp = EXCLUDED.timestamp
WHERE EXCLUDED.revision >= s.revision`

	mock.ExpectExec(regexp.QuoteMeta(expectedSaveQuery)).
		WithArgs(
			stream.Identifier.String(),
			snap.Revision,
			stateBytes,
			sqlmock.AnyArg(),
		).
		WillReturnError(dbErr)

	err = store.Save(ctx, stream, snap)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, dbErr) {
		t.Fatalf("expected dbErr, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled sql expectations: %v", err)
	}
}

func TestSnapshotStore_Load_QueryError(t *testing.T) {
	t.Parallel()

	store, mock := setupSnapshotStore[cartState](t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:cart:c-103"),
	}

	dbErr := errors.New("query failed")
	mock.ExpectQuery(regexp.QuoteMeta("SELECT revision, snapshot FROM snapshots WHERE stream_id = $1")).
		WithArgs(stream.Identifier.String()).
		WillReturnError(dbErr)

	_, err := store.Load(ctx, stream)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, dbErr) {
		t.Fatalf("expected dbErr, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled sql expectations: %v", err)
	}
}

func TestSnapshotStore_Load_UnmarshalError(t *testing.T) {
	t.Parallel()

	store, mock := setupSnapshotStore[cartState](t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:cart:c-104"),
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT revision, snapshot FROM snapshots WHERE stream_id = $1")).
		WithArgs(stream.Identifier.String()).
		WillReturnRows(sqlmock.NewRows([]string{"revision", "snapshot"}).
			AddRow(uint64(1), []byte("invalid-json{")))

	_, err := store.Load(ctx, stream)
	if err == nil {
		t.Fatal("expected unmarshal error, got nil")
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

	mock.ExpectQuery(regexp.QuoteMeta("SELECT revision, snapshot FROM snapshots WHERE stream_id = $1")).
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

	store, mock := setupSnapshotStore[cartState](t, snapstorepg.WithTableName("custom_snapshots"))
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:cart:c-101"),
	}

	// 1. Assert Save with custom table name
	snap := flux.Snapshot[cartState]{
		State:    cartState{Items: []string{"bread"}, Total: 5},
		Revision: 10,
	}
	stateBytes, err := json.Marshal(snap.State)
	if err != nil {
		t.Fatalf("Marshal state failed: %v", err)
	}

	expectedSaveQuery := `INSERT INTO custom_snapshots AS s (stream_id, revision, snapshot, timestamp)
VALUES ($1, $2, $3, $4)
ON CONFLICT (stream_id) DO UPDATE SET
  revision = EXCLUDED.revision,
  snapshot = EXCLUDED.snapshot,
  timestamp = EXCLUDED.timestamp
WHERE EXCLUDED.revision >= s.revision`

	mock.ExpectExec(regexp.QuoteMeta(expectedSaveQuery)).
		WithArgs(
			stream.Identifier.String(),
			snap.Revision,
			stateBytes,
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := store.Save(ctx, stream, snap); err != nil {
		t.Fatalf("Save with custom table name failed: %v", err)
	}

	// 2. Assert Load with custom table name
	mock.ExpectQuery(regexp.QuoteMeta("SELECT revision, snapshot FROM custom_snapshots WHERE stream_id = $1")).
		WithArgs("urn:acme:prod:sales:tenant-1:cart:c-101").
		WillReturnError(sql.ErrNoRows)

	_, err = store.Load(ctx, stream)
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

func TestConstructorAlias(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	s := snapstorepg.NewSnapshotStore[cartState](db)
	if s == nil {
		t.Fatal("expected non-nil SnapshotStore from NewSnapshotStore")
	}
}

func TestValidateTableName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tableName string
		wantErr   bool
	}{
		{name: "standard identifier", tableName: "snapshots", wantErr: false},
		{name: "with underscores and numbers", tableName: "my_snapshots_v2", wantErr: false},
		{name: "starting with underscore", tableName: "_snapshots", wantErr: false},
		{name: "schema qualified", tableName: "public.snapshots", wantErr: false},
		{name: "schema qualified with underscores", tableName: "my_schema.my_snapshots", wantErr: false},
		{name: "double quoted", tableName: `"snapshots"`, wantErr: false},
		{name: "schema qualified double quotes", tableName: `"my_schema"."snapshots"`, wantErr: false},

		// Invalid cases
		{name: "empty string", tableName: "", wantErr: true},
		{name: "semicolon injection", tableName: "snapshots; DROP TABLE users; --", wantErr: true},
		{name: "dash comment injection", tableName: "snapshots--", wantErr: true},
		{name: "inline space", tableName: "snapshots table", wantErr: true},
		{name: "quote injection", tableName: "snapshots' OR '1'='1", wantErr: true},
		{name: "starts with number", tableName: "1snapshots", wantErr: true},
		{name: "invalid characters", tableName: "snapshots$name", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := snapstorepg.ValidateTableName(tt.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTableName(%q) error = %v, wantErr %v", tt.tableName, err, tt.wantErr)
			}
			if tt.wantErr && err != nil && !errors.Is(err, snapstorepg.ErrInvalidTableName) {
				t.Errorf("ValidateTableName(%q) error = %v, want ErrInvalidTableName", tt.tableName, err)
			}
		})
	}
}

func TestWithTableName_PanicsOnInvalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tableName string
	}{
		{name: "semicolon injection", tableName: "snapshots; DROP TABLE users; --"},
		{name: "spaces", tableName: "invalid table name"},
		{name: "dashes", tableName: "table-with-dashes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				r := recover()
				if r == nil {
					t.Errorf("expected WithTableName to panic on invalid name %q", tt.tableName)
				}
			}()
			_ = snapstorepg.WithTableName(tt.tableName)
		})
	}
}
