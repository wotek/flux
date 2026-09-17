package server

import (
	"log/slog"

	"github.com/wotek/flux"
	"github.com/wotek/flux/projection"
)

// Option configures the [Server] instance during initialization.
type Option func(*config)

type config struct {
	httpAddr        string
	eventStore      flux.EventStore
	projectionStore projection.Store
	logger          *slog.Logger
}

// WithHTTP enables an HTTP gateway on the specified listen address (e.g. ":8080").
func WithHTTP(addr string) Option {
	return func(c *config) {
		c.httpAddr = addr
	}
}

// WithEventStore configures a custom event store implementation.
func WithEventStore(store flux.EventStore) Option {
	return func(c *config) {
		c.eventStore = store
	}
}

// WithProjectionStore configures a custom projection checkpoint store.
func WithProjectionStore(store projection.Store) Option {
	return func(c *config) {
		c.projectionStore = store
	}
}

// WithLogger configures a custom structured logger.
func WithLogger(logger *slog.Logger) Option {
	return func(c *config) {
		c.logger = logger
	}
}

