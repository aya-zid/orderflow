# OrderFlow

**Event-driven order processing platform in Go.** Orders are accepted over HTTP, then processed asynchronously by inventory, payment and notification services over RabbitMQ. The project focuses on the failure modes that appear once a monolith is split into services: dual writes, duplicate deliveries, overselling, partial failures and poison messages.

[![CI](https://github.com/aya-zid/orderflow/actions/workflows/ci.yml/badge.svg)](https://github.com/aya-zid/orderflow/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/go-1.23-00ADD8.svg)](https://go.dev)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> **Note on `go.sum`:** this repository snapshot intentionally does not include `go.sum`. Run `make tidy` (or `go mod tidy`) once before building.

---

## Contents

- [Why this project](#why-this-project)
- [Architecture](#architecture)
- [Order lifecycle](#order-lifecycle)
- [Services](#services)
- [Reliability features](#reliability-features)
- [Quick start](#quick-start)
- [API examples](#api-examples)
- [Demo and load testing](#demo-and-load-testing)
- [Failure injection and DLQ replay](#failure-injection-and-dlq-replay)
- [Observability](#observability)
- [Kubernetes](#kubernetes)
- [Testing](#testing)
- [Project layout](#project-layout)
- [Design decisions](#design-decisions)
- [Roadmap](#roadmap)
- [License](#license)

---

## Why this project

A simple CRUD order service is easy. A reliable distributed one is not. OrderFlow is a compact reference implementation that shows how to:

- Commit a business change and its event atomically with the **transactional outbox** pattern.
- Avoid duplicate orders with a database unique constraint plus a Redis-backed **`Idempotency-Key`**.
- Make every consumer **idempotent** using a `processed_events` inbox.
- Reserve stock **atomically** so concurrent orders cannot oversell.
- Close the cancellation-before-reservation race with **advisory locks** and cancellation tombstones.
- Compensate partial failures with a small **saga** (`inventory.reserved` -> payment -> `order.confirmed`, or cancellation -> `inventory.released`).
- Survive transient failures with **retry queues** and isolate poison messages in **DLQs**.
- Replay dead-lettered events with a small operational tool.
- Observe the whole flow with **Prometheus** metrics and a provisioned **Grafana** dashboard.

The stack is deliberately boring: Go, Echo, PostgreSQL, RabbitMQ, Redis, Docker Compose and Kubernetes manifests. No framework magic and no external SaaS dependency.

## Architecture

```mermaid
flowchart LR
    client[Client] -->|HTTP + Idempotency-Key| gateway[Gateway :8080]
    gateway -->|reverse proxy| order[Order :8081]

    order -->|outbox| rabbit[(RabbitMQ topic exchange\norderflow.events)]
    rabbit -->|order.created| inventory[Inventory :8082]
    inventory -->|inventory.reserved / rejected| rabbit
    rabbit -->|inventory.reserved| payment[Payment :8083]
    payment -->|payment.succeeded / failed| rabbit
    rabbit -->|inventory.reserved / rejected\npayment.succeeded / failed| order
    rabbit -->|order.confirmed / cancelled| notification[Notification :8084]

    order --- orderdb[(order_db)]
    inventory --- inventorydb[(inventory_db)]
    payment --- paymentdb[(payment_db)]
    notification --- notificationdb[(notification_db)]

    order --- redis[(Redis)]
    gateway --- redis
    order -.-> prometheus[Prometheus]
    gateway -.-> prometheus
    inventory -.-> prometheus
    payment -.-> prometheus
    notification -.-> prometheus
    prometheus --> grafana[Grafana]
```

Every service owns its database. Services never read each other's tables; the only integration path is the event exchange (plus the gateway's HTTP proxy to the order service).

### Event topology

| Queue | Routing keys | Consumer |
| --- | --- | --- |
| `inventory.order-created` | `order.created` | inventory |
| `inventory.order-cancelled` | `order.cancelled` | inventory |
| `payment.inventory-events` | `inventory.reserved` | payment |
| `order.inventory-events` | `inventory.reserved`, `inventory.rejected` | order |
| `order.payment-events` | `payment.succeeded`, `payment.failed` | order |
| `notification.order-events` | `order.confirmed`, `order.cancelled` | notification |

For each queue and routing key, the consumer declares a retry queue named `<queue>--<routing-key>.retry` with a TTL and dead-lettering back to the exchange. Messages that exceed `MAX_RETRIES` or fail permanently go to `<queue>.dlq`.

## Order lifecycle

```mermaid
stateDiagram-v2
    [*] --> PENDING: order.created
    PENDING --> RESERVED: inventory.reserved
    PENDING --> CANCELLED: inventory.rejected
    RESERVED --> PAID: payment.succeeded
    RESERVED --> CANCELLED: payment.failed / customer cancel
    PAID --> CONFIRMED: payment.succeeded
    PAID --> CANCELLED: customer cancel
    CONFIRMED --> [*]
    CANCELLED --> [*]
```

- `PENDING`: order persisted; waiting for stock reservation.
- `RESERVED`: stock reserved; payment is in progress.
- `PAID`: payment captured; confirmation is being recorded.
- `CONFIRMED`: terminal success state.
- `CANCELLED`: terminal failure or customer cancellation.

Cancelling a `PENDING`, `RESERVED` or `PAID` order emits `order.cancelled`. Inventory consumes that event and releases any reservation it holds. Cancelling a `CONFIRMED` order is rejected with `409 Conflict`.

## Services

| Service | Port | Responsibility | Datastore |
| --- | --- | --- | --- |
| `gateway` | 8080 | Public API, rate limiting, request IDs, Swagger UI, reverse proxy | Redis |
| `order` | 8081 | Order aggregate, state machine, idempotency, outbox, saga coordinator | PostgreSQL, Redis |
| `inventory` | 8082 | Stock levels, atomic reservation and release | PostgreSQL |
| `payment` | 8083 | Simulated payment capture with configurable failure rate | PostgreSQL |
| `notification` | 8084 | Idempotent customer notifications | PostgreSQL |

All services expose:

- `GET /healthz` - process liveness.
- `GET /readyz` - dependency readiness (PostgreSQL, RabbitMQ, Redis where applicable).
- `GET /metrics` - Prometheus metrics.

The gateway additionally exposes `GET /docs` (Swagger UI) and `GET /openapi.yaml`.

## Reliability features

### Transactional outbox

The order, inventory and payment services write the business row and an `outbox` row in the same PostgreSQL transaction. A relay polls unpublished rows with `FOR UPDATE SKIP LOCKED`, publishes them with publisher confirms, and marks them published. If the process crashes after commit but before publish, the relay retries on the next tick. If it crashes after publish but before marking, the event is published twice; consumers tolerate that via the inbox.

### Idempotent HTTP writes

`POST /v1/orders` requires an `Idempotency-Key` header. The service:

1. Returns the cached response if the key was already completed.
2. Takes a short Redis lock to reject concurrent duplicates.
3. Falls back to the database unique constraint on `idempotency_key` if Redis is unavailable.

The same key returns the original order instead of creating a second one.

### Idempotent consumers

Each consumer records `(consumer, event_id)` in a `processed_events` table inside the same transaction as its business change. Redelivered messages are acknowledged without repeating side effects.

### Atomic stock reservation

`inventory.reserve` updates stock with a single conditional `UPDATE ... WHERE available >= quantity`. If no row matches, the reservation is rejected and `inventory.rejected` is emitted. This is safe under concurrent consumers.

### Saga compensation

The happy path is `order.created -> inventory.reserved -> payment.succeeded -> order.confirmed`. Failures cancel the order and emit `order.cancelled`, which triggers `inventory.released`. There is no distributed transaction and no two-phase commit.

### Retries and DLQs

Transient handler errors are republished to a per-binding retry queue with `x-retry-count`. After `MAX_RETRIES`, or immediately for permanent errors (invalid payload, unsupported event type), the message is moved to the consumer's DLQ. `cmd/replay-dlq` republishes DLQ messages after a fix.

## Quick start

### Prerequisites

- Docker Engine 24+ with Docker Compose v2
- (Optional, for local Go development) Go 1.23+

### Run the stack

```bash
cp .env.example .env
make up
```

Or directly:

```bash
docker compose --env-file .env -f deploy/docker/docker-compose.yml up -d --build
```

Wait for the services to become healthy:

```bash
docker compose -f deploy/docker/docker-compose.yml ps
curl http://localhost:8080/readyz
```

Endpoints:

| URL | What |
| --- | --- |
| http://localhost:8080/docs | Swagger UI (served by the gateway) |
| http://localhost:8085 | Standalone Swagger UI container |
| http://localhost:8081 | Order service (internal API) |
| http://localhost:15672 | RabbitMQ management (`orderflow` / `orderflow`) |
| http://localhost:9090 | Prometheus |
| http://localhost:3000 | Grafana (`admin` / `admin`) |
| http://localhost:8080/metrics | Gateway metrics |

Create the first order:

```bash
curl -i -X POST http://localhost:8080/v1/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: 3f1c2a9e-8b7d-4c6a-9e1f-2d3c4b5a6f70' \
  -d '{
    "customer_id": "cust-42",
    "currency": "EUR",
    "items": [
      {"sku": "SKU-1", "quantity": 2, "unit_price_cents": 1999}
    ]
  }'
```

The response is `201 Created` with the order in `PENDING`. Poll it until it reaches `CONFIRMED` or `CANCELLED`:

```bash
curl http://localhost:8080/v1/orders/<order_id>
```

### Stop the stack

```bash
make down
```

`make down` removes containers and volumes, including database data. Use `docker compose ... down` without `-v` to keep volumes.

## API examples

### Create an order

```http
POST /v1/orders
Content-Type: application/json
Idempotency-Key: 3f1c2a9e-8b7d-4c6a-9e1f-2d3c4b5a6f70
```

```json
{
  "customer_id": "cust-42",
  "currency": "EUR",
  "items": [
    { "sku": "SKU-1", "quantity": 2, "unit_price_cents": 1999 }
  ]
}
```

```json
{
  "order_id": "0e4b9d8c-4a4e-4f2b-9a5c-5e7d8f9a0b1c",
  "customer_id": "cust-42",
  "status": "PENDING",
  "currency": "EUR",
  "total_cents": 3998,
  "items": [
    { "sku": "SKU-1", "quantity": 2, "unit_price_cents": 1999 }
  ],
  "version": 0,
  "created_at": "2026-01-01T12:00:00Z",
  "updated_at": "2026-01-01T12:00:00Z",
  "history": [
    { "from": "", "to": "PENDING", "created_at": "2026-01-01T12:00:00Z" }
  ]
}
```

Replaying the same request with the same `Idempotency-Key` returns the same order.

### Get an order

```http
GET /v1/orders/{id}
```

### List orders

```http
GET /v1/orders?customer_id=cust-42&status=CONFIRMED&limit=50&offset=0
```

### Cancel an order

```http
POST /v1/orders/{id}/cancel
Content-Type: application/json

{ "reason": "customer changed their mind" }
```

Cancellation returns `409 Conflict` for `CONFIRMED` orders. Cancelling an already cancelled order is idempotent and returns `200 OK`.

### Errors

Errors use a small JSON body:

```json
{ "error": "order already exists" }
```

| Status | Meaning |
| --- | --- |
| `400` | Invalid input or missing `Idempotency-Key` |
| `404` | Order not found |
| `409` | Duplicate idempotency key or invalid state transition |
| `429` | Gateway rate limit exceeded |
| `502` | Order service unavailable |

## Demo and load testing

### End-to-end demo

```bash
make demo
```

The PowerShell script creates a few orders, polls them, prints the final states, and demonstrates idempotency and cancellation. It uses only `Invoke-RestMethod`, so it works on a clean Windows machine.

### Load generator

```bash
make loadtest
```

This runs 500 orders with 50 concurrent requests against `SKU-1`. The seeded stock is intentionally lower than the order volume, so you will see a mix of `CONFIRMED` and `CANCELLED` orders and can verify in PostgreSQL that `available >= 0` and `reserved >= 0` always hold.

```bash
docker compose -f deploy/docker/docker-compose.yml exec postgres \
  psql -U orderflow -d inventory_db \
  -c "SELECT sku, available, reserved FROM stock ORDER BY sku;"
```

## Failure injection and DLQ replay

### Payment failures

Set the simulated failure rate before starting the stack:

```bash
PAYMENT_FAILURE_RATE=1.0 make up
```

Every payment attempt then fails, orders move to `CANCELLED`, and inventory is released. Set it back to `0.1` (or `0.0`) and restart the payment service to restore the happy path.

### Poison messages

A permanent handler error (for example an unsupported event type) sends the message straight to the consumer DLQ. Inspect queues in the RabbitMQ management UI, or list DLQ depth with `rabbitmqctl`:

```bash
docker compose -f deploy/docker/docker-compose.yml exec rabbitmq \
  rabbitmqctl list_queues name messages | grep dlq
```

### Replay a DLQ

```bash
go run ./cmd/replay-dlq \
  -url amqp://orderflow:orderflow@localhost:5672/ \
  -queue order.inventory-events.dlq \
  -routing-key inventory.reserved \
  -limit 100
```

The replay tool resets `x-retry-count` and sets `x-replayed=true`, so the message gets a fresh retry budget. Replayed events are still deduplicated by the inbox.

## Observability

Prometheus scrapes all five services every 5 seconds. Grafana is provisioned with the **OrderFlow Overview** dashboard:

- HTTP requests per second and p95 latency by service.
- Order creation and confirmation rates.
- Inventory reservation outcomes.
- Outbox backlog by service.
- Cancellations by reason.
- Dead-letter message rate.

The dashboard is defined in `deploy/docker/grafana/dashboards/orderflow.json` and is loaded automatically. No manual import is required.

Useful metric names:

| Metric | Labels | Meaning |
| --- | --- | --- |
| `http_requests_total` | `service`, `method`, `route`, `status` | HTTP traffic |
| `http_request_duration_seconds` | `service`, `method`, `route` | Request latency histogram |
| `orders_created_total` | - | Orders accepted |
| `orders_confirmed_total` | - | Orders reached `CONFIRMED` |
| `orders_cancelled_total` | `reason` | Orders cancelled |
| `inventory_reservations_total` | `result` | Reservation success/rejection |
| `events_published_total` | `event_type` | Outbox events published |
| `outbox_pending` | `service` | Unpublished outbox rows |
| `dlq_messages_total` | `queue` | Messages moved to a DLQ |

## Kubernetes

Kustomize manifests live in `deploy/k8s`:

```bash
kubectl apply -k deploy/k8s
# or the development overlay
kubectl apply -k deploy/k8s/overlays/dev
```

The base includes:

- Namespace, ConfigMap and Secret.
- PostgreSQL (PVC), RabbitMQ and Redis.
- Deployments and Services for all five services.
- Readiness/liveness probes, resource requests/limits, non-root containers.
- HPAs for the gateway and order service.
- Default-deny NetworkPolicies with namespace-internal traffic and DNS egress allowed.

The manifests reference images named `orderflow/<service>:1.0.0`. Build and push them to your registry, or load them into a local cluster (for example `kind load docker-image`), before applying. The Secret contains placeholder passwords and must be replaced for any real deployment.

## Testing

```bash
make test          # unit tests with the race detector
make coverage      # coverage.out
make lint          # go vet + golangci-lint if installed
make integration   # integration tests; the stack must be running
make e2e           # concurrent end-to-end test; the stack must be running
```

Integration tests are guarded by the `integration` build tag and default to `http://localhost:8080`. Override with `ORDERFLOW_GATEWAY_URL`.

Unit tests cover the order state machine, event envelope encoding/decoding, configuration validation and inventory reservation rules. Integration tests exercise the full create -> confirm/cancel flow and idempotency through the gateway.

## Project layout

```text
orderflow/
├── api/                      OpenAPI 3.0 contract + embedded spec
├── cmd/                      Service entrypoints and operational tools
│   ├── gateway/
│   ├── inventory/
│   ├── notification/
│   ├── order/
│   ├── payment/
│   ├── loadgen/              Concurrent order generator
│   └── replay-dlq/           DLQ replay tool
├── deploy/
│   ├── docker/               Dockerfile, Compose, Prometheus, Grafana
│   └── k8s/                  Kustomize base and dev overlay
├── docs/                     Architecture, event flow, operations, ADRs
├── internal/
│   ├── gateway/              Reverse proxy and rate limiting
│   ├── order/                Order domain, app, repository, HTTP adapter
│   ├── inventory/            Stock domain, app, repository
│   ├── payment/              Simulated payment app and repository
│   ├── notification/         Notification app and repository
│   └── platform/             Shared config, logging, DB, RabbitMQ, Redis,
│                             metrics, outbox and HTTP server
├── pkg/events/               Versioned event envelope and payloads
├── scripts/                  PowerShell developer scripts
├── tests/                    Integration and end-to-end tests
├── Makefile
├── go.mod
└── .env.example
```

## Design decisions

| Decision | Rationale | Trade-off |
| --- | --- | --- |
| Transactional outbox | Removes the dual-write gap between PostgreSQL and RabbitMQ | Extra table, relay process and at-least-once delivery |
| At-least-once + inbox | Simpler and more available than exactly-once | Consumers must be idempotent |
| RabbitMQ topic exchange | Flexible routing without a broker per event type | Operational overhead of managing queues and DLQs |
| One database per service | Clear ownership and independent migrations | No cross-service joins; reporting needs a separate path |
| Saga with compensation | Avoids distributed transactions | Temporary inconsistency is visible and must be modeled |
| Redis idempotency cache | Fast duplicate detection at the edge | Redis is not the source of truth; the database constraint is |
| Simulated payment | Keeps the demo self-contained | Not a real PSP integration |

More detail: [architecture](docs/ARCHITECTURE.md), [event flow](docs/EVENT_FLOW.md), [operations](docs/OPERATIONS.md) and the [ADRs](docs/adr).

## Roadmap

- Replace the simulated payment adapter with a real provider and webhook verification.
- Add a reporting/read model fed by events.
- Add OpenTelemetry tracing across HTTP and AMQP.
- Add schema registry or contract tests for event evolution.
- Add a reconciliation job for orders stuck in non-terminal states.
- Add refund compensation for payments captured just before a cancellation.
- Add Kubernetes PodDisruptionBudgets and a proper ingress.

## License

MIT. See [LICENSE](LICENSE).
