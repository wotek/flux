package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/wotek/flux"
	"github.com/wotek/flux/codec"
)

var _ flux.EventStore = (*EventStore)(nil)

// EventStore is a MySQL-backed implementation of [flux.EventStore].
type EventStore struct {
	db         *sql.DB
	serializer codec.Serializer
	config     config
}

// New creates a new MySQL [EventStore].
func New(db *sql.DB, serializer codec.Serializer, opts ...Option) *EventStore {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	return &EventStore{
		db:         db,
		serializer: serializer,
		config:     cfg,
	}
}

// NewEventStore is an alias for [New] to maintain explicit constructor naming.
func NewEventStore(db *sql.DB, serializer codec.Serializer, opts ...Option) *EventStore {
	return New(db, serializer, opts...)
}

// Append adds new events to a specific stream, enforcing optimistic concurrency.
func (s *EventStore) Append(ctx context.Context, stream flux.Stream, expectedRevision uint64, events []flux.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if len(events) == 0 {
		return nil
	}

	streamID := stream.Identifier.String()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting append transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	checkQuery := fmt.Sprintf("SELECT COALESCE(MAX(revision), 0) FROM %s WHERE stream_id = ? FOR UPDATE", s.config.tableName)
	var currentRevision uint64
	if err := tx.QueryRowContext(ctx, checkQuery, streamID).Scan(&currentRevision); err != nil {
		return fmt.Errorf("checking stream revision for %q: %w", streamID, err)
	}

	if currentRevision != expectedRevision {
		return fmt.Errorf("%w: expected revision %d, got %d", flux.ErrConcurrency, expectedRevision, currentRevision)
	}

	insertQuery := fmt.Sprintf(`INSERT INTO %s (
    stream_id, stream_type, event_id, event_type, revision,
    event_data, causation_id, correlation_id, timestamp, application_metadata
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, s.config.tableName)

	stmt, err := tx.PrepareContext(ctx, insertQuery)
	if err != nil {
		return fmt.Errorf("preparing insert statement: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()

	for i, env := range events {
		if env.CreatedAt.IsZero() {
			env.CreatedAt = time.Now().UTC()
		}
		env.Stream = stream
		env.Revision = expectedRevision + uint64(i) + 1

		payload, err := s.serializer.Marshal(env)
		if err != nil {
			return fmt.Errorf("marshaling envelope: %w", err)
		}

		var streamType *string
		if resType := stream.Identifier.ResourceType(); resType != "" {
			streamType = &resType
		}

		var causationID *string
		if !env.CausationIdentifier.IsEmpty() {
			s := env.CausationIdentifier.String()
			causationID = &s
		}

		var correlationID *string
		if !env.CorrelationIdentifier.IsEmpty() {
			s := env.CorrelationIdentifier.String()
			correlationID = &s
		}

		var appMetadata []byte
		if len(env.Metadata) > 0 {
			metaBytes, err := json.Marshal(env.Metadata)
			if err != nil {
				return fmt.Errorf("marshaling application metadata: %w", err)
			}
			appMetadata = metaBytes
		}

		_, err = stmt.ExecContext(ctx,
			streamID,
			streamType,
			env.Identifier.String(),
			env.Event.Name(),
			env.Revision,
			payload,
			causationID,
			correlationID,
			env.CreatedAt,
			appMetadata,
		)
		if err != nil {
			if isDuplicateKeyError(err) {
				return fmt.Errorf("%w: %s", flux.ErrConcurrency, err.Error())
			}
			return fmt.Errorf("inserting event %d into stream %q: %w", env.Revision, streamID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		if isDuplicateKeyError(err) {
			return fmt.Errorf("%w: %s", flux.ErrConcurrency, err.Error())
		}
		return fmt.Errorf("committing append transaction for %q: %w", streamID, err)
	}

	return nil
}

// Read retrieves events for a specific stream starting after the given fromRevision.
// Passing 0 reads the entire stream from the beginning.
func (s *EventStore) Read(ctx context.Context, stream flux.Stream, fromRevision uint64) (flux.StreamIterator, error) {
	streamID := stream.Identifier.String()
	query := fmt.Sprintf("SELECT position, revision, event_data FROM %s WHERE stream_id = ? AND revision > ? ORDER BY revision ASC", s.config.tableName)

	return func(yield func(flux.Envelope, error) bool) {
		rows, err := s.db.QueryContext(ctx, query, streamID, fromRevision)
		if err != nil {
			yield(flux.Envelope{}, fmt.Errorf("querying events for stream %q: %w", streamID, err))
			return
		}
		defer func() {
			_ = rows.Close()
		}()

		for rows.Next() {
			if err := ctx.Err(); err != nil {
				yield(flux.Envelope{}, err)
				return
			}

			var (
				position  uint64
				revision  uint64
				eventData []byte
			)
			if err := rows.Scan(&position, &revision, &eventData); err != nil {
				yield(flux.Envelope{}, fmt.Errorf("scanning event row: %w", err))
				return
			}

			env, err := s.serializer.Unmarshal(eventData)
			if err != nil {
				yield(flux.Envelope{}, fmt.Errorf("unmarshaling event payload: %w", err))
				return
			}

			env.Position = position
			env.Revision = revision
			env.Stream = stream

			if !yield(env, nil) {
				return
			}
		}

		if err := rows.Err(); err != nil {
			yield(flux.Envelope{}, fmt.Errorf("iterating event rows: %w", err))
			return
		}
	}, nil
}

// Stream retrieves events from the global event log starting after the given position.
func (s *EventStore) Stream(ctx context.Context, position uint64) (flux.StreamIterator, error) {
	query := fmt.Sprintf("SELECT position, revision, event_data FROM %s WHERE position > ? ORDER BY position ASC", s.config.tableName)

	return func(yield func(flux.Envelope, error) bool) {
		rows, err := s.db.QueryContext(ctx, query, position)
		if err != nil {
			yield(flux.Envelope{}, fmt.Errorf("querying global event stream: %w", err))
			return
		}
		defer func() {
			_ = rows.Close()
		}()

		for rows.Next() {
			if err := ctx.Err(); err != nil {
				yield(flux.Envelope{}, err)
				return
			}

			var (
				pos       uint64
				revision  uint64
				eventData []byte
			)
			if err := rows.Scan(&pos, &revision, &eventData); err != nil {
				yield(flux.Envelope{}, fmt.Errorf("scanning global event row: %w", err))
				return
			}

			env, err := s.serializer.Unmarshal(eventData)
			if err != nil {
				yield(flux.Envelope{}, fmt.Errorf("unmarshaling event payload: %w", err))
				return
			}

			env.Position = pos
			env.Revision = revision

			if !yield(env, nil) {
				return
			}
		}

		if err := rows.Err(); err != nil {
			yield(flux.Envelope{}, fmt.Errorf("iterating global event rows: %w", err))
			return
		}
	}, nil
}

func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return true
	}
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "1062") || strings.Contains(errMsg, "duplicate entry")
}
