package mysql_test

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/wotek/flux"
	mysqlproj "github.com/wotek/flux/projection/store/mysql"
)

func setupTestStore(t *testing.T, opts ...mysqlproj.Option) (*mysqlproj.Store, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock database: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	store := mysqlproj.New(db, opts...)
	return store, mock
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

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT position FROM checkpoints WHERE consumer_id = ?`)).
		WithArgs(id.String()).
		WillReturnError(errors.New("db connection lost"))

	_, err := store.GetPosition(ctx, id)
	if err == nil {
		t.Fatal("expected error on database query failure")
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

	if _, err := store.GetPosition(ctx, id); err == nil {
		t.Fatal("expected error on cancelled context for GetPosition")
	}
}

func TestUpdate_Success(t *testing.T) {
	t.Parallel()

	store, mock := setupTestStore(t)
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:u1")
	env := flux.Envelope{Position: 100}

	mock.ExpectBegin()
	// Read-model mutation inside tx
	mock.ExpectExec(regexp.QuoteMeta("UPDATE my_projection SET count = count + 1")).
		WillReturnResult(sqlmock.NewResult(1, 1))

	expectedUpsert := `INSERT INTO checkpoints (consumer_id, position)
VALUES (?, ?)
ON DUPLICATE KEY UPDATE
    position = IF(VALUES(position) >= position, VALUES(position), position),
    updated_at = IF(VALUES(position) >= position, VALUES(updated_at), updated_at)`

	mock.ExpectExec(regexp.QuoteMeta(expectedUpsert)).
		WithArgs(id.String(), uint64(100)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	err := store.Update(ctx, id, env, func(txCtx context.Context) error {
		tx, ok := mysqlproj.TxFromContext(txCtx)
		if !ok {
			return errors.New("expected tx in context")
		}
		_, err := tx.ExecContext(txCtx, "UPDATE my_projection SET count = count + 1")
		return err
	})

	if err != nil {
		t.Fatalf("unexpected Update error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

func TestUpdate_MutateFails_RollsBack(t *testing.T) {
	t.Parallel()

	store, mock := setupTestStore(t)
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:u2")
	env := flux.Envelope{Position: 100}

	mock.ExpectBegin()
	mock.ExpectRollback()

	expectedErr := errors.New("read-model constraint violated")
	err := store.Update(ctx, id, env, func(txCtx context.Context) error {
		return expectedErr
	})

	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error wrapping %v, got %v", expectedErr, err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

func TestUpdate_CommitError(t *testing.T) {
	t.Parallel()

	store, mock := setupTestStore(t)
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:u3")
	env := flux.Envelope{Position: 100}

	mock.ExpectBegin()
	expectedUpsert := `INSERT INTO checkpoints (consumer_id, position)
VALUES (?, ?)
ON DUPLICATE KEY UPDATE
    position = IF(VALUES(position) >= position, VALUES(position), position),
    updated_at = IF(VALUES(position) >= position, VALUES(updated_at), updated_at)`

	mock.ExpectExec(regexp.QuoteMeta(expectedUpsert)).
		WithArgs(id.String(), uint64(100)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit().WillReturnError(errors.New("deadlock on commit"))

	err := store.Update(ctx, id, env, func(txCtx context.Context) error {
		return nil
	})

	if err == nil {
		t.Fatal("expected error on commit failure")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

func TestUpdate_CheckpointUpsertError_RollsBack(t *testing.T) {
	t.Parallel()

	store, mock := setupTestStore(t)
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:upsert_err")
	env := flux.Envelope{Position: 100}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE my_projection SET count = count + 1")).
		WillReturnResult(sqlmock.NewResult(1, 1))

	expectedUpsert := `INSERT INTO checkpoints (consumer_id, position)
VALUES (?, ?)
ON DUPLICATE KEY UPDATE
    position = IF(VALUES(position) >= position, VALUES(position), position),
    updated_at = IF(VALUES(position) >= position, VALUES(updated_at), updated_at)`

	expectedErr := errors.New("disk full on checkpoint write")
	mock.ExpectExec(regexp.QuoteMeta(expectedUpsert)).
		WithArgs(id.String(), uint64(100)).
		WillReturnError(expectedErr)

	mock.ExpectRollback()

	err := store.Update(ctx, id, env, func(txCtx context.Context) error {
		tx, ok := mysqlproj.TxFromContext(txCtx)
		if !ok {
			return errors.New("expected tx in context")
		}
		_, err := tx.ExecContext(txCtx, "UPDATE my_projection SET count = count + 1")
		return err
	})

	if err == nil {
		t.Fatal("expected error on checkpoint upsert failure, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error wrapping %v, got %v", expectedErr, err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

func TestUpdate_BeginTxError(t *testing.T) {
	t.Parallel()

	store, mock := setupTestStore(t)
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:u4")
	env := flux.Envelope{Position: 100}

	mock.ExpectBegin().WillReturnError(errors.New("too many connections"))

	err := store.Update(ctx, id, env, func(txCtx context.Context) error {
		return nil
	})

	if err == nil {
		t.Fatal("expected error on begin tx failure")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

func TestUpdate_ContextCancelled(t *testing.T) {
	t.Parallel()

	store, _ := setupTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:u5")
	env := flux.Envelope{Position: 100}

	err := store.Update(ctx, id, env, func(txCtx context.Context) error {
		return nil
	})

	if err == nil {
		t.Fatal("expected error on cancelled context for Update")
	}
}

func TestWithCheckpointsTable_CustomName(t *testing.T) {
	t.Parallel()

	store, mock := setupTestStore(t, mysqlproj.WithCheckpointsTable("custom_proj_checkpoints"))
	ctx := context.Background()
	id := flux.MustParseIdentifier("urn:acme:prod:projector:1:worker:custom")
	env := flux.Envelope{Position: 55}

	mock.ExpectBegin()
	expectedUpsert := `INSERT INTO custom_proj_checkpoints (consumer_id, position)
VALUES (?, ?)
ON DUPLICATE KEY UPDATE
    position = IF(VALUES(position) >= position, VALUES(position), position),
    updated_at = IF(VALUES(position) >= position, VALUES(updated_at), updated_at)`

	mock.ExpectExec(regexp.QuoteMeta(expectedUpsert)).
		WithArgs(id.String(), uint64(55)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	err := store.Update(ctx, id, env, func(txCtx context.Context) error {
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected Update error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

func TestWithCheckpointsTable_InvalidNamePanics(t *testing.T) {
	t.Parallel()

	invalidNames := []string{
		"checkpoints; DROP TABLE users; --",
		"invalid name with spaces",
		"table-with-dashes",
		"",
	}

	for _, name := range invalidNames {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				r := recover()
				if r == nil {
					t.Errorf("expected panic for invalid table name %q, got nil", name)
				}
			}()

			_ = mysqlproj.WithCheckpointsTable(name)
		})
	}
}

func TestTxFromContext_Missing(t *testing.T) {
	t.Parallel()

	tx, ok := mysqlproj.TxFromContext(context.Background())
	if ok || tx != nil {
		t.Fatalf("expected nil tx and false, got %v, %v", tx, ok)
	}
}

func TestConstructorAlias(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	s := mysqlproj.NewStore(db)
	if s == nil {
		t.Fatal("expected non-nil Store from NewStore")
	}
}

func TestWithTableNameAlias(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	s := mysqlproj.New(db, mysqlproj.WithTableName("app_checkpoints"))
	if s == nil {
		t.Fatal("expected non-nil Store from New with WithTableName")
	}
}
