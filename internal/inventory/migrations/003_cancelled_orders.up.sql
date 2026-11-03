-- Cancellation tombstones close the race where order.cancelled is consumed
-- before order.created. The reservation path checks this table and refuses to
-- reserve stock for an order that has already been cancelled.
CREATE TABLE IF NOT EXISTS cancelled_orders (
    order_id   uuid PRIMARY KEY,
    reason     text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now()
);
