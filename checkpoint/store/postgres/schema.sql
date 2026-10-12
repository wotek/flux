CREATE TABLE IF NOT EXISTS checkpoints (
    consumer_id TEXT NOT NULL,
    position BIGINT NOT NULL, -- non-negative invariant: position >= 0
    updated_at TIMESTAMPTZ(6) NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_id)
);
