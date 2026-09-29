package mysql_test

import (
	"context"
	_ "embed"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/wotek/flux"
	checkpointmysql "github.com/wotek/flux/checkpoint/store/mysql"
)

//go:embed schema.sql
var schemaSQL string

func setupTestStore(t *testing.T, opts ...checkpointmysql.Option) (*checkpointmysql.Store, sqlmock.Sqlmock) {
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

	store := checkpointmysql.New(db, opts...)
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
		t.Fatalf("unexpected error executing schema: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

func TestGetPosition_Existing(t *testing.T) {
	t.Parallel()

	store, mock := setupTestStore(t)
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:w1")

	expectedQuery := `SELECT position FROM checkpoints WHERE consumer_id = ?`
	mock.ExpectQuery(regexp.QuoteMeta(expectedQuery)).
		WithArgs(id.String()).
		WillReturnRows(sqlmock.NewRows([]string{"position"}).AddRow(uint64(42)))

	pos, err := store.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pos != 42 {
		t.Fatalf("expected position 42, got %d", pos)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

func TestGetPosition_MissingReturnsZero(t *testing.T) {
	t.Parallel()

	store, mock := setupTestStore(t)
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:w2")

	expectedQuery := `SELECT position FROM checkpoints WHERE consumer_id = ?`
	mock.ExpectQuery(regexp.QuoteMeta(expectedQuery)).
		WithArgs(id.String()).
		WillReturnRows(sqlmock.NewRows([]string{"position"})) // empty result => sql.ErrNoRows

	pos, err := store.GetPosition(ctx, id)
	if err != nil {
		t.Fatalf("expected no error for missing consumer, got: %v", err)
	}
	if pos != 0 {
		t.Fatalf("expected position 0 for missing consumer, got %d", pos)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

func TestGetPosition_QueryError(t *testing.T) {
	t.Parallel()

	store, mock := setupTestStore(t)
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:w3")

	dbErr := errors.New("connection reset by peer")
	expectedQuery := `SELECT position FROM checkpoints WHERE consumer_id = ?`
	mock.ExpectQuery(regexp.QuoteMeta(expectedQuery)).
		WithArgs(id.String()).
		WillReturnError(dbErr)

	_, err := store.GetPosition(ctx, id)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, dbErr) {
		t.Fatalf("expected %v, got %v", dbErr, err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

func TestGetPosition_ContextCancelled(t *testing.T) {
	t.Parallel()

	store, _ := setupTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:w4")
	_, err := store.GetPosition(ctx, id)
	if err == nil {
		t.Fatal("expected error on cancelled context, got nil")
	}
}

func TestSetPosition_Success(t *testing.T) {
	t.Parallel()

	store, mock := setupTestStore(t)
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:w5")

	expectedQuery := `INSERT INTO checkpoints (consumer_id, position)
VALUES (?, ?)
ON DUPLICATE KEY UPDATE
    position = IF(VALUES(position) >= position, VALUES(position), position),
    updated_at = IF(VALUES(position) >= position, VALUES(updated_at), updated_at)`

	mock.ExpectExec(regexp.QuoteMeta(expectedQuery)).
		WithArgs(id.String(), uint64(105)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := store.SetPosition(ctx, id, 105); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

func TestSetPosition_ExecError(t *testing.T) {
	t.Parallel()

	store, mock := setupTestStore(t)
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:w6")

	dbErr := errors.New("deadlock found when trying to get lock")
	expectedQuery := `INSERT INTO checkpoints (consumer_id, position)
VALUES (?, ?)
ON DUPLICATE KEY UPDATE
    position = IF(VALUES(position) >= position, VALUES(position), position),
    updated_at = IF(VALUES(position) >= position, VALUES(updated_at), updated_at)`

	mock.ExpectExec(regexp.QuoteMeta(expectedQuery)).
		WithArgs(id.String(), uint64(105)).
		WillReturnError(dbErr)

	err := store.SetPosition(ctx, id, 105)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, dbErr) {
		t.Fatalf("expected %v, got %v", dbErr, err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

func TestSetPosition_ContextCancelled(t *testing.T) {
	t.Parallel()

	store, _ := setupTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:w7")
	if err := store.SetPosition(ctx, id, 10); err == nil {
		t.Fatal("expected error on cancelled context, got nil")
	}
}

func TestWithTableName(t *testing.T) {
	t.Parallel()

	t.Run("valid custom table name", func(t *testing.T) {
		t.Parallel()
		store, mock := setupTestStore(t, checkpointmysql.WithTableName("custom_checkpoints"))
		ctx := context.Background()
		id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:w8")

		expectedQuery := `SELECT position FROM custom_checkpoints WHERE consumer_id = ?`
		mock.ExpectQuery(regexp.QuoteMeta(expectedQuery)).
			WithArgs(id.String()).
			WillReturnRows(sqlmock.NewRows([]string{"position"}).AddRow(uint64(99)))

		pos, err := store.GetPosition(ctx, id)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pos != 99 {
			t.Fatalf("expected 99, got %d", pos)
		}
	})

	t.Run("constructor alias NewStore", func(t *testing.T) {
		t.Parallel()
		db, _, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to open sqlmock: %v", err)
		}
		defer func() { _ = db.Close() }()

		s := checkpointmysql.NewStore(db)
		if s == nil {
			t.Fatal("expected non-nil Store from NewStore")
		}
	})

	t.Run("invalid table name panics", func(t *testing.T) {
		t.Parallel()
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic on invalid table name")
			}
		}()
		_ = checkpointmysql.WithTableName("checkpoints; DROP TABLE users;")
	})
}
