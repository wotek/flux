package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/checkpoint"
)

var _ checkpoint.Store = (*Store)(nil)

// Store is a MySQL-backed implementation of [checkpoint.Store].
type Store struct {
	db     *sql.DB
	config config
}

// New creates a new MySQL [Store].
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
// for the consumer identified by id from MySQL.
// Returns 0 and nil error if the consumer has no stored checkpoint.
func (s *Store) GetPosition(ctx context.Context, id flux.Identifier) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	consumerID := id.String()
	query := fmt.Sprintf("SELECT position FROM %s WHERE consumer_id = ?", s.config.tableName)

	var position uint64
	err := s.db.QueryRowContext(ctx, query, consumerID).Scan(&position)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("getting checkpoint position from mysql for %q: %w", id, err)
	}

	return position, nil
}

// SetPosition records that the consumer identified by id has successfully
// processed the global stream through position in MySQL using monotonic max semantics.
// Older positions will not overwrite newer ones.
func (s *Store) SetPosition(ctx context.Context, id flux.Identifier, position uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	consumerID := id.String()
	query := fmt.Sprintf(`INSERT INTO %s (consumer_id, position)
VALUES (?, ?)
ON DUPLICATE KEY UPDATE
    position = IF(VALUES(position) >= position, VALUES(position), position),
    updated_at = IF(VALUES(position) >= position, VALUES(updated_at), updated_at)`, s.config.tableName)

	_, err := s.db.ExecContext(ctx, query, consumerID, position)
	if err != nil {
		return fmt.Errorf("saving checkpoint position to mysql for %q: %w", id, err)
	}

	return nil
}
