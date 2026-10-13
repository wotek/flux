package postgres_test

import (
	"context"
	_ "embed"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/wotek/flux"
	checkpointpostgres "github.com/wotek/flux/checkpoint/store/postgres"
)

//go:embed schema.sql
var schemaSQL string

func setupTestStore(t *testing.T, opts ...checkpointpostgres.Option) (*checkpointpostgres.Store, sqlmock.Sqlmock) {
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

	store := checkpointpostgres.New(db, opts...)
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

	expectedQuery := `SELECT position FROM checkpoints WHERE consumer_id = $1`
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

	expectedQuery := `SELECT position FROM checkpoints WHERE consumer_id = $1`
	mock.ExpectQuery(regexp.QuoteMeta(expectedQuery)).
		WithArgs(id.String()).
		WillReturnRows(sqlmock.NewRows([]string{"position"}))

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
	expectedQuery := `SELECT position FROM checkpoints WHERE consumer_id = $1`
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
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestSetPosition_Success(t *testing.T) {
	t.Parallel()

	store, mock := setupTestStore(t)
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:w5")

	expectedQuery := `INSERT INTO checkpoints AS c (consumer_id, position)
VALUES ($1, $2)
ON CONFLICT (consumer_id) DO UPDATE SET
  position = CASE
    WHEN EXCLUDED.position >= c.position THEN EXCLUDED.position
    ELSE c.position
  END,
  updated_at = CASE
    WHEN EXCLUDED.position >= c.position THEN now()
    ELSE c.updated_at
  END`

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

	dbErr := errors.New("deadlock detected")
	expectedQuery := `INSERT INTO checkpoints AS c (consumer_id, position)
VALUES ($1, $2)
ON CONFLICT (consumer_id) DO UPDATE SET
  position = CASE
    WHEN EXCLUDED.position >= c.position THEN EXCLUDED.position
    ELSE c.position
  END,
  updated_at = CASE
    WHEN EXCLUDED.position >= c.position THEN now()
    ELSE c.updated_at
  END`

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
	err := store.SetPosition(ctx, id, 10)
	if err == nil {
		t.Fatal("expected error on cancelled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestWithTableName(t *testing.T) {
	t.Parallel()

	t.Run("valid custom table name in GetPosition", func(t *testing.T) {
		t.Parallel()
		store, mock := setupTestStore(t, checkpointpostgres.WithTableName("custom_checkpoints"))
		ctx := context.Background()
		id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:w8")

		expectedQuery := `SELECT position FROM custom_checkpoints WHERE consumer_id = $1`
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

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet mock expectations: %v", err)
		}
	})

	t.Run("valid schema-qualified table name in SetPosition", func(t *testing.T) {
		t.Parallel()
		store, mock := setupTestStore(t, checkpointpostgres.WithTableName("public.checkpoints"))
		ctx := context.Background()
		id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:w9")

		expectedQuery := `INSERT INTO public.checkpoints AS c (consumer_id, position)
VALUES ($1, $2)
ON CONFLICT (consumer_id) DO UPDATE SET
  position = CASE
    WHEN EXCLUDED.position >= c.position THEN EXCLUDED.position
    ELSE c.position
  END,
  updated_at = CASE
    WHEN EXCLUDED.position >= c.position THEN now()
    ELSE c.updated_at
  END`

		mock.ExpectExec(regexp.QuoteMeta(expectedQuery)).
			WithArgs(id.String(), uint64(200)).
			WillReturnResult(sqlmock.NewResult(1, 1))

		if err := store.SetPosition(ctx, id, 200); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet mock expectations: %v", err)
		}
	})

	t.Run("constructor alias NewStore", func(t *testing.T) {
		t.Parallel()
		db, _, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to open sqlmock: %v", err)
		}
		defer func() { _ = db.Close() }()

		s := checkpointpostgres.NewStore(db)
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
		_ = checkpointpostgres.WithTableName("checkpoints; DROP TABLE users;")
	})
}

func TestValidateTableName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tableName string
		wantErr   bool
	}{
		{name: "standard identifier", tableName: "checkpoints", wantErr: false},
		{name: "with underscores and numbers", tableName: "my_checkpoints_v2", wantErr: false},
		{name: "starting with underscore", tableName: "_checkpoints", wantErr: false},
		{name: "schema qualified", tableName: "public.checkpoints", wantErr: false},
		{name: "schema qualified with underscores", tableName: "my_schema.my_checkpoints", wantErr: false},
		{name: "double quoted", tableName: `"checkpoints"`, wantErr: false},
		{name: "schema qualified double quotes", tableName: `"my_schema"."checkpoints"`, wantErr: false},

		// Invalid cases
		{name: "empty string", tableName: "", wantErr: true},
		{name: "semicolon injection", tableName: "checkpoints; DROP TABLE users; --", wantErr: true},
		{name: "dash comment injection", tableName: "checkpoints--", wantErr: true},
		{name: "inline space", tableName: "checkpoints table", wantErr: true},
		{name: "quote injection", tableName: "checkpoints' OR '1'='1", wantErr: true},
		{name: "starts with number", tableName: "1checkpoints", wantErr: true},
		{name: "invalid characters", tableName: "checkpoints$name", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := checkpointpostgres.ValidateTableName(tt.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTableName(%q) error = %v, wantErr %v", tt.tableName, err, tt.wantErr)
			}
			if tt.wantErr && err != nil && !errors.Is(err, checkpointpostgres.ErrInvalidTableName) {
				t.Errorf("ValidateTableName(%q) error = %v, want ErrInvalidTableName", tt.tableName, err)
			}
		})
	}
}
