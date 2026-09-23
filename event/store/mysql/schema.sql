CREATE TABLE IF NOT EXISTS events (
    position BIGINT AUTO_INCREMENT PRIMARY KEY,
    stream_id VARCHAR(255) NOT NULL,
    stream_type VARCHAR(255) NULL,
    event_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    revision BIGINT UNSIGNED NOT NULL,
    event_data LONGBLOB NOT NULL,
    causation_id VARCHAR(255) NULL,
    correlation_id VARCHAR(255) NULL,
    timestamp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL,
    application_metadata JSON NULL,

    -- Optimistic Concurrency Constraint
    UNIQUE KEY uk_stream_revision (stream_id, revision),

    -- Observability Indexes
    INDEX idx_event_type (event_type),
    INDEX idx_correlation (correlation_id)
);
