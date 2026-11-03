CREATE TABLE IF NOT EXISTS inventory (
    sku        text PRIMARY KEY,
    available  integer NOT NULL CHECK (available >= 0),
    reserved   integer NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    version    integer NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reservations (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    group_id   uuid NOT NULL,
    order_id   uuid NOT NULL,
    sku        text NOT NULL,
    quantity   integer NOT NULL CHECK (quantity > 0),
    status     text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (order_id, sku)
);

CREATE INDEX IF NOT EXISTS idx_reservations_order
    ON reservations (order_id);

CREATE INDEX IF NOT EXISTS idx_reservations_group
    ON reservations (group_id);

CREATE INDEX IF NOT EXISTS idx_reservations_status
    ON reservations (status);

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
