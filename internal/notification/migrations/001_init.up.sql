CREATE TABLE IF NOT EXISTS notifications (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id   uuid NOT NULL,
    type       text NOT NULL,
    recipient  text NOT NULL,
    payload    jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_notifications_order
    ON notifications (order_id, created_at DESC);

CREATE TABLE IF NOT EXISTS processed_events (
    consumer     text NOT NULL,
    event_id     uuid NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);
