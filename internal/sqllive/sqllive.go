package sqllive

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	tc "github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// DefaultPostgresImage is the official PostgreSQL image used for integration tests.
	DefaultPostgresImage = "postgres:16-alpine"

	// DefaultMySQLImage is the official MySQL 8 image used for integration tests.
	DefaultMySQLImage = "mysql:8.4"
)

// OpenPostgres starts a PostgreSQL testcontainer (DefaultPostgresImage) and returns
// a connected *sql.DB using the pgx stdlib driver.
// The caller is responsible for invoking the returned cleanup function (e.g. via t.Cleanup).
func OpenPostgres(ctx context.Context) (*sql.DB, func(), error) {
	pgContainer, err := tcpostgres.Run(ctx,
		DefaultPostgresImage,
		tcpostgres.WithDatabase("testdb"),
		tcpostgres.WithUsername("testuser"),
		tcpostgres.WithPassword("testpass"),
		tc.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("starting postgres container: %w", err)
	}

	cleanup := func() {
		// Use independent timeout context for termination
		termCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = pgContainer.Terminate(termCtx)
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("getting postgres connection string: %w", err)
	}

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("opening pgx database: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		cleanup()
		return nil, nil, fmt.Errorf("pinging postgres database: %w", err)
	}

	combinedCleanup := func() {
		_ = db.Close()
		cleanup()
	}

	return db, combinedCleanup, nil
}

// OpenMySQL starts a MySQL testcontainer (DefaultMySQLImage) and returns
// a connected *sql.DB using the go-sql-driver/mysql driver with parseTime=true.
// The caller is responsible for invoking the returned cleanup function (e.g. via t.Cleanup).
func OpenMySQL(ctx context.Context) (*sql.DB, func(), error) {
	mysqlContainer, err := tcmysql.Run(ctx,
		DefaultMySQLImage,
		tcmysql.WithDatabase("testdb"),
		tcmysql.WithUsername("testuser"),
		tcmysql.WithPassword("testpass"),
		tc.WithWaitStrategy(
			wait.ForLog("port: 3306  MySQL Community Server").
				WithOccurrence(1).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("starting mysql container: %w", err)
	}

	cleanup := func() {
		termCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = mysqlContainer.Terminate(termCtx)
	}

	connStr, err := mysqlContainer.ConnectionString(ctx, "parseTime=true", "multiStatements=true")
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("getting mysql connection string: %w", err)
	}

	db, err := sql.Open("mysql", connStr)
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("opening mysql database: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		cleanup()
		return nil, nil, fmt.Errorf("pinging mysql database: %w", err)
	}

	combinedCleanup := func() {
		_ = db.Close()
		cleanup()
	}

	return db, combinedCleanup, nil
}

// MustExecSchema executes a SQL script containing one or more statements against db.
// If any statement execution fails, it calls t.Fatalf.
func MustExecSchema(t testing.TB, db *sql.DB, schemaSQL string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Execute individual statements split by ';' or whole script
	// Standard database/sql drivers may require executing statement-by-statement
	// unless multiStatements=true is supported.
	statements := splitStatements(schemaSQL)
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("failed executing schema statement %q: %v", stmt, err)
		}
	}
}

// splitStatements separates SQL DDL by semicolons while ignoring semicolons inside quotes or comments.
func splitStatements(sqlScript string) []string {
	var stmts []string
	var current strings.Builder

	inSingleQuote := false
	inDoubleQuote := false
	inLineComment := false
	inBlockComment := false

	chars := []rune(sqlScript)
	n := len(chars)

	for i := 0; i < n; i++ {
		c := chars[i]
		next := rune(0)
		if i+1 < n {
			next = chars[i+1]
		}

		if inLineComment {
			current.WriteRune(c)
			if c == '\n' {
				inLineComment = false
			}
			continue
		}

		if inBlockComment {
			current.WriteRune(c)
			if c == '*' && next == '/' {
				current.WriteRune(next)
				i++
				inBlockComment = false
			}
			continue
		}

		if !inSingleQuote && !inDoubleQuote {
			if c == '-' && next == '-' {
				current.WriteRune(c)
				current.WriteRune(next)
				i++
				inLineComment = true
				continue
			}
			if c == '/' && next == '*' {
				current.WriteRune(c)
				current.WriteRune(next)
				i++
				inBlockComment = true
				continue
			}
		}

		if c == '\'' && !inDoubleQuote {
			inSingleQuote = !inSingleQuote
		} else if c == '"' && !inSingleQuote {
			inDoubleQuote = !inDoubleQuote
		}

		if c == ';' && !inSingleQuote && !inDoubleQuote {
			s := strings.TrimSpace(current.String())
			if s != "" {
				stmts = append(stmts, s)
			}
			current.Reset()
			continue
		}

		current.WriteRune(c)
	}

	if s := strings.TrimSpace(current.String()); s != "" {
		stmts = append(stmts, s)
	}

	return stmts
}
