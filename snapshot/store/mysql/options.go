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

// Option configures a MySQL [SnapshotStore].
type Option func(*config)

type config struct {
	tableName string
}

func defaultConfig() config {
	return config{
		tableName: "snapshots",
	}
}

// WithTableName sets a custom table name for the snapshots table.
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
