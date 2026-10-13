package postgres

import (
	"database/sql"
)

// Store is a PostgreSQL-backed implementation of [projection.Store] that runs
// read-model mutations and checkpoint position updates in a single database transaction.
type Store struct {
	db     *sql.DB
	config config
}

// New creates a new PostgreSQL projection [Store].
func New(db *sql.DB, opts ...Option) *Store {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	return &Store{
		db:     db,
		config: cfg,
	}
}

// NewStore is an alias for [New] to maintain explicit constructor naming across packages.
func NewStore(db *sql.DB, opts ...Option) *Store {
	return New(db, opts...)
}
