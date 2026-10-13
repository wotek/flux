package postgres

import (
	"database/sql"
)

// SnapshotStore is a PostgreSQL-backed implementation of [flux.SnapshotStore].
type SnapshotStore[S any] struct {
	db     *sql.DB
	config config
}

// New creates a new PostgreSQL [SnapshotStore] for state type S.
func New[S any](db *sql.DB, opts ...Option) *SnapshotStore[S] {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	return &SnapshotStore[S]{
		db:     db,
		config: cfg,
	}
}

// NewSnapshotStore is an alias for [New] to maintain explicit constructor naming.
func NewSnapshotStore[S any](db *sql.DB, opts ...Option) *SnapshotStore[S] {
	return New[S](db, opts...)
}
