CREATE TABLE IF NOT EXISTS payments (
    id             uuid PRIMARY KEY,
    order_id       uuid NOT NULL UNIQUE,
    amount_cents   bigint NOT NULL CHECK (amount_cents >= 0),
    currency       char(3) NOT NULL,
    status         text NOT NULL,
    failure_reason text NOT NULL DEFAULT '',
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_payments_order ON payments (order_id);

CREATE TABLE IF NOT EXISTS outbox (
    id           uuid PRIMARY KEY,
    aggregate_id uuid NOT NULL,
    event_type   text NOT NULL,
    payload      jsonb NOT NULL,
    headers      jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    retry_count  integer NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox (created_at)
    WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS processed_events (
    consumer     text NOT NULL,
    event_id     uuid NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);
