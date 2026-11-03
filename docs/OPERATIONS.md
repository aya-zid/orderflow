# Operations

Runbook for local development and for understanding what a production on-call rotation would watch.

## Local lifecycle

```bash
cp .env.example .env
make up                 # build images and start the stack
make ps                 # container status
make logs               # follow all logs
make down               # stop and remove containers + volumes
```

`make up` uses `docker compose --env-file .env -f deploy/docker/docker-compose.yml up -d --build`.

### Readiness

```bash
curl -fsS http://localhost:8080/readyz
curl -fsS http://localhost:8081/readyz
curl -fsS http://localhost:8082/readyz
curl -fsS http://localhost:8083/readyz
curl -fsS http://localhost:8084/readyz
```

`/readyz` returns `503` with the failing dependency name. `/healthz` only proves the process is alive.

### Service logs

```bash
docker compose -f deploy/docker/docker-compose.yml logs -f order
docker compose -f deploy/docker/docker-compose.yml logs -f inventory payment
```

Logs are structured JSON. Useful fields:

- `request_id`, `correlation_id`
- `event_type`, `event_id`, `message_id`
- `order_id`, `sku`, `payment_id`
- `error`

Correlate an HTTP request with its downstream events using `correlation_id`.

## Common incidents

| Symptom | Likely cause | Action |
| --- | --- | --- |
| Order stuck in `PENDING` | Inventory consumer down, DLQ, or no stock event | Check `inventory` readiness and logs; inspect `inventory.order-created` queue and DLQ |
| Order stuck in `RESERVED` | Payment consumer down or payment retries | Check `payment` logs, `payment.inventory-events` queue and DLQ |
| Order stuck in `PAID` | Order consumer cannot apply `payment.succeeded` | Check `order.payment-events` queue/DLQ and order service logs |
| Duplicate order created | Client changed `Idempotency-Key`; Redis and DB both bypassed | Verify the key is stable; inspect `orders.idempotency_key` |
| `429` from gateway | Per-IP fixed window exceeded | Raise `RATE_LIMIT_PER_MINUTE` or wait for the next minute |
| `502` from gateway | Order service unreachable | Check order readiness and gateway logs |
| Inventory count negative | Should be impossible (`CHECK` constraints) | Treat as a bug; stop consumers and investigate the transaction log |
| Outbox backlog grows | RabbitMQ down or publisher confirms failing | Check broker health and relay logs; do not delete outbox rows |
| DLQ depth grows | Poison message or persistent downstream failure | Inspect headers (`x-last-error`, `x-dlq-reason`), fix, replay |
| Payment failure spike | `PAYMENT_FAILURE_RATE` set too high or simulation enabled | Lower the rate and restart payment |
| Payment captured for a cancelled order | Customer cancelled while payment was in flight | Refund manually; automated refund compensation is out of scope |

### Inspect queues

```bash
docker compose -f deploy/docker/docker-compose.yml exec rabbitmq \
  rabbitmqctl list_queues name messages consumers
```

Look for:

- `inventory.order-created` and `inventory.order-cancelled`
- `payment.inventory-events`
- `order.inventory-events` and `order.payment-events`
- `notification.order-events`
- `*.dlq`

The RabbitMQ management UI at http://localhost:15672 shows bindings, message rates and message headers.

### Inspect the outbox

```bash
docker compose -f deploy/docker/docker-compose.yml exec postgres \
  psql -U orderflow -d order_db -c \
  "SELECT event_type, count(*) FROM outbox WHERE published_at IS NULL GROUP BY event_type;"
```

A small, non-zero backlog is normal during a relay tick. A backlog that grows for minutes means publishing is failing. Never delete rows to "fix" the backlog; the relay owns them.

### Inspect an order

```bash
docker compose -f deploy/docker/docker-compose.yml exec postgres \
  psql -U orderflow -d order_db -c \
  "SELECT id, status, version, updated_at FROM orders ORDER BY created_at DESC LIMIT 10;"

docker compose -f deploy/docker/docker-compose.yml exec postgres \
  psql -U orderflow -d order_db -c \
  "SELECT from_status, to_status, reason, event_id, created_at
   FROM order_status_history WHERE order_id = '<order-id>' ORDER BY id;"
```

### Inspect inventory

```bash
docker compose -f deploy/docker/docker-compose.yml exec postgres \
  psql -U orderflow -d inventory_db -c \
  "SELECT sku, available, reserved FROM inventory ORDER BY sku;"

docker compose -f deploy/docker/docker-compose.yml exec postgres \
  psql -U orderflow -d inventory_db -c \
  "SELECT order_id, sku, quantity, status FROM reservations WHERE order_id = '<order-id>';"
```

Invariants:

- `available >= 0`
- `reserved >= 0`
- `available + reserved` only changes when stock is added or released, never through a reservation.

Cancellation tombstones:

```sql
SELECT order_id, reason, created_at FROM cancelled_orders ORDER BY created_at DESC LIMIT 20;
```

Reservation and cancellation take a transaction-scoped advisory lock keyed by `order_id`. If an order is cancelled before inventory processes its creation event, the tombstone makes the later reservation a no-op.

### Inspect payments and notifications

```bash
docker compose -f deploy/docker/docker-compose.yml exec postgres \
  psql -U orderflow -d payment_db -c \
  "SELECT order_id, status, amount_cents, currency, failure_reason, created_at
   FROM payments ORDER BY created_at DESC LIMIT 20;"

docker compose -f deploy/docker/docker-compose.yml exec postgres \
  psql -U orderflow -d notification_db -c \
  "SELECT order_id, type, recipient, created_at
   FROM notifications ORDER BY created_at DESC LIMIT 20;"
```

## DLQ replay

1. Inspect the DLQ and the message headers.
2. Fix the root cause (bad payload, missing SKU, schema mismatch, downstream bug).
3. Replay a bounded batch:

```bash
go run ./cmd/replay-dlq \
  -url amqp://orderflow:orderflow@localhost:5672/ \
  -queue order.inventory-events.dlq \
  -routing-key inventory.reserved \
  -limit 50
```

The tool resets `x-retry-count` to `0` and sets `x-replayed=true`. Replayed events still go through the inbox, so a message that was already processed will not repeat its side effect.

Use the routing key that matches the original event type. If a DLQ contains mixed event types, inspect each message and replay with the correct key; do not blindly replay everything with one key.

## Payment failure injection

```bash
PAYMENT_FAILURE_RATE=1.0 make up
```

Expected behavior:

1. Orders are created and stock is reserved.
2. Payment records a failed attempt and emits `payment.failed`.
3. Order becomes `CANCELLED`.
4. Inventory releases the reservation.

Restore with:

```bash
PAYMENT_FAILURE_RATE=0.0 make up
```

The variable is read by the payment service at startup.

## Metrics and alerts

Prometheus: http://localhost:9090
Grafana: http://localhost:3000 (`admin` / `admin`)

Suggested alerts (not provisioned as rules in this repo):

| Alert | Expression | Meaning |
| --- | --- | --- |
| Outbox backlog | `max(outbox_pending) > 1000 for 5m` | Relay is not draining |
| DLQ growth | `sum(rate(dlq_messages_total[5m])) > 0` | Poison messages or persistent failure |
| High error rate | `sum(rate(http_requests_total{status=~"5.."}[5m])) / sum(rate(http_requests_total[5m])) > 0.01` | 5xx spike |
| Latency | `histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le)) > 0.5` | p95 above 500 ms |
| No consumers | `rabbitmq_queue_consumers{queue=~".*order.*"} == 0` | Consumer down (requires RabbitMQ exporter) |
| Payment failures | `sum(rate(orders_cancelled_total{reason="payment"}[5m])) > 1` | Payment failure spike |

The dashboard covers the main application signals without extra exporters.

## Scaling

- **Gateway and order**: stateless; scale horizontally. The order service serializes transitions with database row locks, so replicas are safe.
- **Inventory and payment**: scale consumers horizontally. RabbitMQ distributes deliveries; the inbox and conditional updates keep side effects safe.
- **Notification**: stateless consumer; scale freely.
- **Outbox relay**: each service runs its own relay. Multiple replicas use `FOR UPDATE SKIP LOCKED`, so batches do not overlap.
- **PostgreSQL**: the local Compose file uses one instance. Production should use managed PostgreSQL with backups, connection pooling and read replicas where needed.
- **RabbitMQ**: use quorum queues for stronger durability in production. The local setup uses classic durable queues for simplicity.

## Database migrations

Migrations are embedded in each service and applied at startup by `internal/platform/postgres.Migrate`. The `schema_migrations` table records applied files in lexical order.

Rules:

- Never edit an applied migration; add a new one.
- Migrations run inside a transaction.
- Keep them additive and backward compatible so old and new replicas can run together during a rolling deploy.
- The inventory cancellation tombstone is migration `003_cancelled_orders.up.sql`.

## Backup and restore (local)

```bash
docker compose -f deploy/docker/docker-compose.yml exec -T postgres \
  pg_dump -U orderflow -d order_db > order_db.sql
```

Restore into a fresh volume:

```bash
docker compose -f deploy/docker/docker-compose.yml exec -T postgres \
  psql -U orderflow -d order_db < order_db.sql
```

The Compose stack uses named volumes. `make down` removes them; use plain `docker compose down` to keep data.

## Kubernetes

```bash
kubectl apply -k deploy/k8s
kubectl -n orderflow get pods,svc,hpa
kubectl -n orderflow logs deploy/order -f
```

The base manifests are production-shaped but reference local image names. Replace the Secret values and image registry before applying to a real cluster. NetworkPolicies assume a CNI that enforces them (Calico, Cilium, etc.).

## Security notes

- The API has no authentication. Put it behind an authenticated ingress or API gateway before exposing it.
- Replace all placeholder secrets in `.env` and `deploy/k8s/secret.yaml`.
- Use TLS at the edge and mTLS between services in a real deployment.
- Rotate the RabbitMQ and PostgreSQL credentials.
- Do not log request bodies or idempotency keys in production.
- The GitHub token used during development must never be committed. Rotate it if it was shared.
