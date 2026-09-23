package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/wotek/flux"
)

var (
	// ErrSnapshotNotFound is returned when no snapshot exists for a stream.
	ErrSnapshotNotFound = errors.New("snapshot not found")
)

var _ flux.SnapshotStore[any] = (*SnapshotStore[any])(nil)

// SnapshotStore is a MySQL-backed implementation of [flux.SnapshotStore].
type SnapshotStore[S any] struct {
	db     *sql.DB
	config config
}

// New creates a new MySQL [SnapshotStore] for state type S.
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

// Load retrieves the latest snapshot for the specified stream from MySQL.
// Returns [ErrSnapshotNotFound] if no snapshot is found.
func (s *SnapshotStore[S]) Load(ctx context.Context, stream flux.Stream) (flux.Snapshot[S], error) {
	if err := ctx.Err(); err != nil {
		return flux.Snapshot[S]{}, err
	}

	streamID := stream.Identifier.String()
	query := fmt.Sprintf("SELECT revision, snapshot FROM %s WHERE stream_id = ?", s.config.tableName)

	var (
		revision uint64
		blob     []byte
	)

	err := s.db.QueryRowContext(ctx, query, streamID).Scan(&revision, &blob)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return flux.Snapshot[S]{}, fmt.Errorf("loading snapshot for %q: %w", stream.Identifier, ErrSnapshotNotFound)
		}
		return flux.Snapshot[S]{}, fmt.Errorf("getting snapshot from mysql for %q: %w", stream.Identifier, err)
	}

	var state S
	if err := json.Unmarshal(blob, &state); err != nil {
		return flux.Snapshot[S]{}, fmt.Errorf("unmarshaling snapshot state for %q: %w", stream.Identifier, err)
	}

	return flux.Snapshot[S]{
		State:    state,
		Revision: revision,
	}, nil
}

// Save persists a snapshot for the specified stream in MySQL using INSERT ... ON DUPLICATE KEY UPDATE.
func (s *SnapshotStore[S]) Save(ctx context.Context, stream flux.Stream, snap flux.Snapshot[S]) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	data, err := json.Marshal(snap.State)
	if err != nil {
		return fmt.Errorf("marshaling snapshot for %q: %w", stream.Identifier, err)
	}

	streamID := stream.Identifier.String()
	query := fmt.Sprintf(`INSERT INTO %s (stream_id, revision, snapshot, timestamp)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
    revision = VALUES(revision),
    snapshot = VALUES(snapshot),
    timestamp = VALUES(timestamp)`, s.config.tableName)

	now := time.Now().UTC()
	_, err = s.db.ExecContext(ctx, query, streamID, snap.Revision, data, now)
	if err != nil {
		return fmt.Errorf("saving snapshot to mysql for %q: %w", stream.Identifier, err)
	}

	return nil
}
