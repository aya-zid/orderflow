CREATE TABLE IF NOT EXISTS orders (
    id              uuid PRIMARY KEY,
    customer_id     text NOT NULL,
    status          text NOT NULL,
    currency        char(3) NOT NULL,
    total_cents     bigint NOT NULL CHECK (total_cents >= 0),
    idempotency_key text NOT NULL UNIQUE,
    version         integer NOT NULL DEFAULT 0,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_created
    ON orders (customer_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_orders_status_created
    ON orders (status, created_at DESC);

CREATE TABLE IF NOT EXISTS order_items (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id         uuid NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    sku              text NOT NULL,
    quantity         integer NOT NULL CHECK (quantity > 0),
    unit_price_cents bigint NOT NULL CHECK (unit_price_cents >= 0),
    UNIQUE (order_id, sku)
);

CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items (order_id);

CREATE TABLE IF NOT EXISTS order_status_history (
    id          bigserial PRIMARY KEY,
    order_id    uuid NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    from_status text NOT NULL DEFAULT '',
    to_status   text NOT NULL,
    reason      text NOT NULL DEFAULT '',
    event_id    uuid,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_order_history_order
    ON order_status_history (order_id, id);

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
