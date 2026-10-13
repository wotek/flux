package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/checkpoint"
)

var _ checkpoint.Store = (*Store)(nil)

// Store is a PostgreSQL-backed implementation of [checkpoint.Store].
type Store struct {
	db     *sql.DB
	config config
}

// New creates a new PostgreSQL [Store].
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

// GetPosition loads the last successfully processed global stream position
// for the consumer identified by id from PostgreSQL.
// Returns 0 and nil error if the consumer has no stored checkpoint.
func (s *Store) GetPosition(ctx context.Context, id flux.Identifier) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	consumerID := id.String()
	query := fmt.Sprintf("SELECT position FROM %s WHERE consumer_id = $1", s.config.tableName)

	var position uint64
	err := s.db.QueryRowContext(ctx, query, consumerID).Scan(&position)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("getting checkpoint position from postgres for %q: %w", id, err)
	}

	return position, nil
}

// SetPosition records that the consumer identified by id has successfully
// processed the global stream through position in PostgreSQL using monotonic max semantics.
// Older positions will not overwrite newer ones.
func (s *Store) SetPosition(ctx context.Context, id flux.Identifier, position uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	consumerID := id.String()
	query := fmt.Sprintf(`INSERT INTO %s AS c (consumer_id, position)
VALUES ($1, $2)
ON CONFLICT (consumer_id) DO UPDATE SET
  position = CASE
    WHEN EXCLUDED.position >= c.position THEN EXCLUDED.position
    ELSE c.position
  END,
  updated_at = CASE
    WHEN EXCLUDED.position >= c.position THEN now()
    ELSE c.updated_at
  END`, s.config.tableName)

	_, err := s.db.ExecContext(ctx, query, consumerID, position)
	if err != nil {
		return fmt.Errorf("saving checkpoint position to postgres for %q: %w", id, err)
	}

	return nil
}
