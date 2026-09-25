package mysql

import (
	"errors"
	"fmt"
	"regexp"
)

// ErrInvalidTableName indicates that a MySQL table name is not a valid SQL identifier.
var ErrInvalidTableName = errors.New("invalid table name")

var validIdentifierRegex = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*|` + "`" + `[A-Za-z_][A-Za-z0-9_]*` + "`" + `)(\.([A-Za-z_][A-Za-z0-9_]*|` + "`" + `[A-Za-z_][A-Za-z0-9_]*` + "`" + `))?$`)

// ValidateTableName checks whether a table name is a valid, safe SQL identifier.
func ValidateTableName(name string) error {
	if !validIdentifierRegex.MatchString(name) {
		return fmt.Errorf("%w: %q (must be an alphanumeric SQL identifier, optionally schema-qualified)", ErrInvalidTableName, name)
	}
	return nil
}

// Option configures a MySQL [EventStore].
type Option func(*config)

type config struct {
	tableName string
	batchSize int
}

func defaultConfig() config {
	return config{
		tableName: "events",
		batchSize: 100,
	}
}

// WithTableName sets a custom table name for the events table.
// It validates that the table name is a safe SQL identifier to prevent SQL injection,
// and panics if the table name is invalid.
func WithTableName(name string) Option {
	if err := ValidateTableName(name); err != nil {
		panic(fmt.Errorf("mysql: %w", err))
	}
	return func(c *config) {
		c.tableName = name
	}
}

// WithBatchSize sets the pagination batch size for Read and Stream queries.
// Defaults to 100 if unset or <= 0.
func WithBatchSize(size int) Option {
	return func(c *config) {
		c.batchSize = size
	}
}
