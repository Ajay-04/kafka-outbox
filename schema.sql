-- Transactional outbox schema (Postgres).
-- The producer writes the business row AND the outbox row in ONE transaction.
-- The relay publishes unpublished rows and marks them published only after ack.

CREATE TABLE IF NOT EXISTS outbox (
    id             UUID PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    payload        JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ,                 -- NULL = not yet published
    claimed_by     TEXT,                        -- relay instance holding the claim
    claimed_at     TIMESTAMPTZ                  -- NULL = unclaimed
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox (created_at) WHERE published_at IS NULL;

-- Consumer-side dedupe. Same idea as an idempotency key: the message id
-- is the dedupe key, so redeliveries are absorbed, never re-applied.
CREATE TABLE IF NOT EXISTS processed_messages (
    message_id   UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The relay's claim query. Run one per poll inside a transaction, with a
-- distinct claimed_by per relay instance. SKIP LOCKED lets N relay
-- instances poll concurrently without ever receiving the same row.
--
--   UPDATE outbox
--   SET claimed_by = $1, claimed_at = now()
--   WHERE id IN (
--       SELECT id FROM outbox
--       WHERE published_at IS NULL
--         AND (claimed_by IS NULL OR claimed_at < now() - interval '5 minutes')
--       ORDER BY created_at
--       LIMIT $2
--       FOR UPDATE SKIP LOCKED
--   )
--   RETURNING id, aggregate_type, aggregate_id, event_type, payload;
--
-- The 5-minute stale-claim window reclaims rows from crashed relays.
