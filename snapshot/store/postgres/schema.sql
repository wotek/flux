CREATE TABLE IF NOT EXISTS snapshots (
    stream_id TEXT PRIMARY KEY,
    revision BIGINT NOT NULL, -- non-negative invariant: revision >= 0
    snapshot BYTEA NOT NULL,
    timestamp TIMESTAMPTZ(6) NOT NULL DEFAULT now()
);
