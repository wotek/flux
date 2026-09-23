package mysql_test

import (
	"context"
	_ "embed"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"

	"github.com/wotek/flux"
	jsoncodec "github.com/wotek/flux/codec/json"
	"github.com/wotek/flux/event"
	mysqlstore "github.com/wotek/flux/event/store/mysql"
)

//go:embed schema.sql
var schemaSQL string

type orderPlaced struct {
	OrderNumber string `json:"order_number"`
	Amount      int    `json:"amount"`
}

func (orderPlaced) Name() string {
	return "OrderPlaced"
}

func setupTestStore(t *testing.T, opts ...mysqlstore.Option) (*mysqlstore.EventStore, sqlmock.Sqlmock, *event.Types, *jsoncodec.Serializer) {
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

	registry := event.NewTypes()
	event.RegisterType[orderPlaced](registry)
	serializer := jsoncodec.New(registry)

	store := mysqlstore.New(db, serializer, opts...)
	return store, mock, registry, serializer
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

func TestEventStore_AppendAndRead(t *testing.T) {
	t.Parallel()

	store, mock, _, serializer := setupTestStore(t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:order:ord-101"),
	}

	env1 := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:event:evt-1"),
		Event: &orderPlaced{
			OrderNumber: "ORD-1",
			Amount:      100,
		},
		Actor: flux.Actor{
			Identifier: flux.MustParseIdentifier("urn:acme:prod:iam:tenant-1:user:usr-1"),
		},
		CorrelationIdentifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:cmd:c-1"),
		CausationIdentifier:   flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:cmd:c-0"),
		Metadata: map[string]string{
			"trace": "123",
		},
	}

	// 1. Mock Append
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COALESCE(MAX(revision), 0) FROM events WHERE stream_id = ? FOR UPDATE")).
		WithArgs(stream.Identifier.String()).
		WillReturnRows(sqlmock.NewRows([]string{"max_rev"}).AddRow(0))

	mock.ExpectPrepare(regexp.QuoteMeta("INSERT INTO events"))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO events")).
		WithArgs(
			stream.Identifier.String(),
			"order",
			env1.Identifier.String(),
			"OrderPlaced",
			uint64(1),
			sqlmock.AnyArg(), // event_data bytes
			env1.CausationIdentifier.String(),
			env1.CorrelationIdentifier.String(),
			sqlmock.AnyArg(), // timestamp
			sqlmock.AnyArg(), // application_metadata JSON
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := store.Append(ctx, stream, 0, []flux.Envelope{env1}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Prepare serialized payload for Read mock
	env1.Stream = stream
	env1.Revision = 1
	env1.CreatedAt = time.Date(2026, 9, 23, 14, 0, 0, 0, time.UTC)
	payloadBytes, err := serializer.Marshal(env1)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// 2. Mock Read
	mock.ExpectQuery(regexp.QuoteMeta("SELECT position, revision, event_data FROM events WHERE stream_id = ? AND revision > ? ORDER BY revision ASC")).
		WithArgs(stream.Identifier.String(), uint64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"position", "revision", "event_data"}).
			AddRow(uint64(42), uint64(1), payloadBytes))

	iter, err := store.Read(ctx, stream, 0)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	var readEvents []flux.Envelope
	for env, iterErr := range iter {
		if iterErr != nil {
			t.Fatalf("iteration error: %v", iterErr)
		}
		readEvents = append(readEvents, env)
	}

	if len(readEvents) != 1 {
		t.Fatalf("expected 1 event, got %d", len(readEvents))
	}
	if readEvents[0].Position != 42 {
		t.Errorf("Position = %d, want 42", readEvents[0].Position)
	}
	if readEvents[0].Revision != 1 {
		t.Errorf("Revision = %d, want 1", readEvents[0].Revision)
	}
	if readEvents[0].Stream != stream {
		t.Errorf("Stream = %v, want %v", readEvents[0].Stream, stream)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled sql expectations: %v", err)
	}
}

func TestEventStore_OptimisticConcurrency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupMock  func(sqlmock.Sqlmock, string)
		wantTarget error
	}{
		{
			name: "revision mismatch in SELECT check",
			setupMock: func(mock sqlmock.Sqlmock, streamID string) {
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta("SELECT COALESCE(MAX(revision), 0) FROM events WHERE stream_id = ? FOR UPDATE")).
					WithArgs(streamID).
					WillReturnRows(sqlmock.NewRows([]string{"max_rev"}).AddRow(5))
				mock.ExpectRollback()
			},
			wantTarget: flux.ErrConcurrency,
		},
		{
			name: "mysql duplicate key error 1062 on insert",
			setupMock: func(mock sqlmock.Sqlmock, streamID string) {
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta("SELECT COALESCE(MAX(revision), 0) FROM events WHERE stream_id = ? FOR UPDATE")).
					WithArgs(streamID).
					WillReturnRows(sqlmock.NewRows([]string{"max_rev"}).AddRow(0))
				mock.ExpectPrepare(regexp.QuoteMeta("INSERT INTO events"))
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO events")).
					WillReturnError(&mysql.MySQLError{
						Number:  1062,
						Message: "Duplicate entry for key 'uk_stream_revision'",
					})
				mock.ExpectRollback()
			},
			wantTarget: flux.ErrConcurrency,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store, mock, _, _ := setupTestStore(t)
			ctx := context.Background()

			stream := flux.Stream{
				Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:order:ord-concur"),
			}
			env := flux.Envelope{
				Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:event:evt-1"),
				Event:      &orderPlaced{OrderNumber: "ORD-C", Amount: 50},
			}

			tt.setupMock(mock, stream.Identifier.String())

			err := store.Append(ctx, stream, 0, []flux.Envelope{env})
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !errors.Is(err, tt.wantTarget) {
				t.Fatalf("expected error wrapping %v, got: %v", tt.wantTarget, err)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unfulfilled sql expectations: %v", err)
			}
		})
	}
}

func TestEventStore_StreamGlobal(t *testing.T) {
	t.Parallel()

	store, mock, _, serializer := setupTestStore(t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:order:ord-glob"),
	}
	env := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:event:evt-g1"),
		Stream:     stream,
		Event:      &orderPlaced{OrderNumber: "ORD-G", Amount: 200},
		Revision:   1,
	}
	payloadBytes, err := serializer.Marshal(env)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT position, revision, event_data FROM events WHERE position > ? ORDER BY position ASC")).
		WithArgs(uint64(10)).
		WillReturnRows(sqlmock.NewRows([]string{"position", "revision", "event_data"}).
			AddRow(uint64(11), uint64(1), payloadBytes))

	iter, err := store.Stream(ctx, 10)
	if err != nil {
		t.Fatalf("Stream failed: %v", err)
	}

	var results []flux.Envelope
	for e, iterErr := range iter {
		if iterErr != nil {
			t.Fatalf("iteration error: %v", iterErr)
		}
		results = append(results, e)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Position != 11 {
		t.Errorf("Position = %d, want 11", results[0].Position)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled sql expectations: %v", err)
	}
}

func TestEventStore_EmptyStream(t *testing.T) {
	t.Parallel()

	store, mock, _, _ := setupTestStore(t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:order:ord-empty"),
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT position, revision, event_data FROM events WHERE stream_id = ? AND revision > ? ORDER BY revision ASC")).
		WithArgs(stream.Identifier.String(), uint64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"position", "revision", "event_data"}))

	iter, err := store.Read(ctx, stream, 0)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	count := 0
	for _, iterErr := range iter {
		if iterErr != nil {
			t.Fatalf("unexpected iteration error: %v", iterErr)
		}
		count++
	}

	if count != 0 {
		t.Errorf("expected 0 events, got %d", count)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled sql expectations: %v", err)
	}
}

func TestEventStore_WithCustomTableName(t *testing.T) {
	t.Parallel()

	store, mock, _, _ := setupTestStore(t, mysqlstore.WithTableName("custom_events"))
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:order:ord-101"),
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT position, revision, event_data FROM custom_events WHERE stream_id = ? AND revision > ? ORDER BY revision ASC")).
		WithArgs("urn:acme:prod:sales:tenant-1:order:ord-101", uint64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"position", "revision", "event_data"}))

	iter, err := store.Read(ctx, stream, 0)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	for range iter {
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled sql expectations: %v", err)
	}
}

func TestEventStore_EmptyAppend(t *testing.T) {
	t.Parallel()

	store, _, _, _ := setupTestStore(t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:order:ord-empty"),
	}

	if err := store.Append(ctx, stream, 0, nil); err != nil {
		t.Fatalf("Append(nil) failed: %v", err)
	}
	if err := store.Append(ctx, stream, 0, []flux.Envelope{}); err != nil {
		t.Fatalf("Append([]) failed: %v", err)
	}
}

func TestEventStore_ContextCancellation(t *testing.T) {
	t.Parallel()

	store, _, _, _ := setupTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:order:ord-canc"),
	}
	env := flux.Envelope{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:event:evt-c1"),
		Event:      &orderPlaced{OrderNumber: "ORD-C", Amount: 10},
	}

	if err := store.Append(ctx, stream, 0, []flux.Envelope{env}); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestEventStore_ReadDeferredQuery(t *testing.T) {
	t.Parallel()

	store, mock, _, _ := setupTestStore(t)
	ctx := context.Background()

	stream := flux.Stream{
		Identifier: flux.MustParseIdentifier("urn:acme:prod:sales:tenant-1:order:ord-lazy"),
	}

	// 1. Obtaining iterator without iterating does not query the DB (preventing connection leaks).
	iter, err := store.Read(ctx, stream, 0)
	if err != nil {
		t.Fatalf("Read returned error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expected no SQL calls before iterator invocation: %v", err)
	}

	// 2. When iterator is invoked, query error is yielded.
	mock.ExpectQuery(regexp.QuoteMeta("SELECT position, revision, event_data FROM events WHERE stream_id = ? AND revision > ? ORDER BY revision ASC")).
		WithArgs("urn:acme:prod:sales:tenant-1:order:ord-lazy", uint64(0)).
		WillReturnError(errors.New("db connection failure"))

	var yieldedErr error
	for _, itErr := range iter {
		if itErr != nil {
			yieldedErr = itErr
			break
		}
	}
	if yieldedErr == nil || yieldedErr.Error() != "querying events for stream \"urn:acme:prod:sales:tenant-1:order:ord-lazy\": db connection failure" {
		t.Errorf("expected query error yielded, got %v", yieldedErr)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled sql expectations: %v", err)
	}
}

func TestEventStore_StreamDeferredQuery(t *testing.T) {
	t.Parallel()

	store, mock, _, _ := setupTestStore(t)
	ctx := context.Background()

	// 1. Obtaining iterator without iterating does not query the DB.
	iter, err := store.Stream(ctx, 10)
	if err != nil {
		t.Fatalf("Stream returned error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expected no SQL calls before iterator invocation: %v", err)
	}

	// 2. When iterator is invoked, query error is yielded.
	mock.ExpectQuery(regexp.QuoteMeta("SELECT position, revision, event_data FROM events WHERE position > ? ORDER BY position ASC")).
		WithArgs(uint64(10)).
		WillReturnError(errors.New("db stream failure"))

	var yieldedErr error
	for _, itErr := range iter {
		if itErr != nil {
			yieldedErr = itErr
			break
		}
	}
	if yieldedErr == nil || yieldedErr.Error() != "querying global event stream: db stream failure" {
		t.Errorf("expected query error yielded, got %v", yieldedErr)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled sql expectations: %v", err)
	}
}
