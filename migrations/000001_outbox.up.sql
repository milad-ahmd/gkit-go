-- Outbox events table for reliable event publishing.
CREATE TABLE IF NOT EXISTS outbox_events (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    topic        TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);

-- Partial index: only unpublished rows; keeps the index small and fast.
CREATE INDEX IF NOT EXISTS outbox_events_unpublished
    ON outbox_events (created_at)
    WHERE published_at IS NULL;
