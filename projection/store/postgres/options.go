package postgres

import (
	"errors"
	"fmt"
	"regexp"
)

// ErrInvalidTableName indicates that a PostgreSQL table name is not a valid SQL identifier.
var ErrInvalidTableName = errors.New("invalid table name")

var validIdentifierRegex = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*|"[A-Za-z_][A-Za-z0-9_]*")(\.([A-Za-z_][A-Za-z0-9_]*|"[A-Za-z_][A-Za-z0-9_]*"))?$`)

// ValidateTableName checks whether a table name is a valid, safe SQL identifier.
func ValidateTableName(name string) error {
	if !validIdentifierRegex.MatchString(name) {
		return fmt.Errorf("%w: %q (must be an alphanumeric SQL identifier, optionally schema-qualified)", ErrInvalidTableName, name)
	}
	return nil
}

// Option configures a PostgreSQL projection [Store].
type Option func(*config)

type config struct {
	tableName string
}

func defaultConfig() config {
	return config{
		tableName: "checkpoints",
	}
}

// WithCheckpointsTable sets a custom table name for the checkpoints table.
// It validates that the table name is a safe SQL identifier to prevent SQL injection,
// and panics if the table name is invalid.
func WithCheckpointsTable(name string) Option {
	if err := ValidateTableName(name); err != nil {
		panic(fmt.Errorf("postgres: %w", err))
	}
	return func(c *config) {
		c.tableName = name
	}
}

// WithTableName is an alias for [WithCheckpointsTable] for consistency across PostgreSQL stores.
func WithTableName(name string) Option {
	return WithCheckpointsTable(name)
}
