-- Event store: append-only event log.
CREATE TABLE IF NOT EXISTS events (
    stream_id   TEXT        NOT NULL,
    version     BIGINT      NOT NULL,
    type        TEXT        NOT NULL,
    data        JSONB       NOT NULL,
    metadata    JSONB       NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stream_id, version)
);

-- Snapshots for faster aggregate reconstruction.
CREATE TABLE IF NOT EXISTS snapshots (
    stream_id   TEXT        PRIMARY KEY,
    version     BIGINT      NOT NULL,
    data        JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
