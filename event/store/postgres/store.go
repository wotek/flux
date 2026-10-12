package postgres

import (
	"database/sql"

	"github.com/wotek/flux/codec"
)

// EventStore is a PostgreSQL-backed implementation of [flux.EventStore].
type EventStore struct {
	db         *sql.DB
	serializer codec.Serializer
	config     config
}

// New creates a new PostgreSQL [EventStore].
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
