package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/wotek/flux"
	"github.com/wotek/flux/projection"
)

var _ projection.Store = (*Store)(nil)

// Store is a MySQL-backed implementation of [projection.Store] that runs
// read-model mutations and checkpoint position updates in a single database transaction.
type Store struct {
	db     *sql.DB
	config config
}

// New creates a new MySQL projection [Store].
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
		return 0, fmt.Errorf("getting projection checkpoint position from mysql for %q: %w", id, err)
	}

	return position, nil
}

// Update executes mutate within a database transaction and, on success,
// updates the checkpoint position using monotonic max semantics before committing.
// If mutate returns an error or the transaction fails, the transaction is rolled back
// and the checkpoint position remains unchanged.
func (s *Store) Update(ctx context.Context, id flux.Identifier, env flux.Envelope, mutate func(txCtx context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning projection transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	txCtx := ContextWithTx(ctx, tx)
	if err := mutate(txCtx); err != nil {
		return fmt.Errorf("executing projection mutation: %w", err)
	}

	consumerID := id.String()
	query := fmt.Sprintf(`INSERT INTO %s (consumer_id, position)
VALUES (?, ?)
ON DUPLICATE KEY UPDATE
    position = IF(VALUES(position) >= position, VALUES(position), position),
    updated_at = IF(VALUES(position) >= position, VALUES(updated_at), updated_at)`, s.config.tableName)

	if _, err := tx.ExecContext(txCtx, query, consumerID, env.Position); err != nil {
		return fmt.Errorf("saving checkpoint position in transaction for %q: %w", id, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing projection transaction: %w", err)
	}

	return nil
}
