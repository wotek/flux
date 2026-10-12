CREATE TABLE IF NOT EXISTS events (
    position BIGSERIAL PRIMARY KEY,
    stream_id TEXT NOT NULL,
    stream_type TEXT NULL,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    revision BIGINT NOT NULL, -- non-negative invariant: revision >= 0
    event_data BYTEA NOT NULL,
    causation_id TEXT NULL,
    correlation_id TEXT NULL,
    timestamp TIMESTAMPTZ(6) DEFAULT now() NOT NULL,
    application_metadata JSONB NULL,

    -- Optimistic Concurrency Constraint
    CONSTRAINT uk_stream_revision UNIQUE (stream_id, revision),

    -- Point Lookup and Idempotency Constraint
    CONSTRAINT uk_stream_event UNIQUE (stream_id, event_id)
);

-- Observability Indexes
CREATE INDEX IF NOT EXISTS idx_event_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_correlation ON events (correlation_id);
