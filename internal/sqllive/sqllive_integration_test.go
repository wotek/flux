//go:build integration

package sqllive_test

import (
	"context"
	"testing"
	"time"

	"github.com/wotek/flux/internal/sqllive"
)

func TestOpenPostgres_Smoke(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, cleanup, err := sqllive.OpenPostgres(ctx)
	if err != nil {
		t.Fatalf("OpenPostgres failed: %v", err)
	}
	t.Cleanup(cleanup)

	var val int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&val); err != nil {
		t.Fatalf("query SELECT 1 failed on postgres: %v", err)
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}

	const testSchema = `
CREATE TABLE smoke_pg (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
INSERT INTO smoke_pg (name) VALUES ('flux');
`
	sqllive.MustExecSchema(t, db, testSchema)

	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM smoke_pg").Scan(&count); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}
}

func TestOpenMySQL_Smoke(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, cleanup, err := sqllive.OpenMySQL(ctx)
	if err != nil {
		t.Fatalf("OpenMySQL failed: %v", err)
	}
	t.Cleanup(cleanup)

	var val int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&val); err != nil {
		t.Fatalf("query SELECT 1 failed on mysql: %v", err)
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}

	const testSchema = `
CREATE TABLE smoke_my (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);
INSERT INTO smoke_my (name) VALUES ('flux');
`
	sqllive.MustExecSchema(t, db, testSchema)

	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM smoke_my").Scan(&count); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}
}
