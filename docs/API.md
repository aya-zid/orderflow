# API

The public API is served by the gateway on port `8080` and proxied to the order service. The order service also exposes the same routes on port `8081` for internal use.

- Base URL: `http://localhost:8080`
- Content type: `application/json`
- Authentication: none in this project (see [operations](OPERATIONS.md#security-notes))
- OpenAPI: [`api/openapi.yaml`](../api/openapi.yaml), Swagger UI at `/docs`

## Idempotency

`POST /v1/orders` requires an `Idempotency-Key` header. Use a UUID or another high-entropy value and keep it stable across retries of the same logical request.

Behavior:

| Situation | Result |
| --- | --- |
| First request with a key | Order is created, `201 Created` |
| Same key, completed response cached | Cached response replayed |
| Same key, first request still running | `409 Conflict` |
| Same key, Redis unavailable | Database unique constraint returns the existing order |
| Different key, same body | A new order is created |

The key is scoped globally, not per customer. Do not reuse a key for a different order.

## Create an order

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

Validation rules:

- `customer_id` is required and trimmed.
- `currency` must be exactly three letters; it is upper-cased.
- At least one item is required.
- `sku` is required and must be unique within the order.
- `quantity` must be positive.
- `unit_price_cents` must be non-negative.

Response `201 Created`:

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

The order is accepted for asynchronous processing. Poll `GET /v1/orders/{id}` to follow it to `CONFIRMED` or `CANCELLED`.

### PowerShell

```powershell
$body = @{
  customer_id = "cust-42"
  currency    = "EUR"
  items       = @(@{ sku = "SKU-1"; quantity = 2; unit_price_cents = 1999 })
} | ConvertTo-Json -Depth 5

$order = Invoke-RestMethod -Method Post `
  -Uri http://localhost:8080/v1/orders `
  -Headers @{ "Idempotency-Key" = [guid]::NewGuid().ToString() } `
  -ContentType "application/json" `
  -Body $body

$order.order_id
```

## Get an order

```http
GET /v1/orders/{id}
```

Response `200 OK`:

```json
{
  "order_id": "0e4b9d8c-4a4e-4f2b-9a5c-5e7d8f9a0b1c",
  "customer_id": "cust-42",
  "status": "CONFIRMED",
  "currency": "EUR",
  "total_cents": 3998,
  "items": [
    { "sku": "SKU-1", "quantity": 2, "unit_price_cents": 1999 }
  ],
  "version": 3,
  "created_at": "2026-01-01T12:00:00Z",
  "updated_at": "2026-01-01T12:00:03Z",
  "history": [
    { "from": "", "to": "PENDING", "created_at": "2026-01-01T12:00:00Z" },
    { "from": "PENDING", "to": "RESERVED", "reason": "inventory reserved", "event_id": "…", "created_at": "2026-01-01T12:00:01Z" },
    { "from": "RESERVED", "to": "PAID", "reason": "payment captured", "event_id": "…", "created_at": "2026-01-01T12:00:02Z" },
    { "from": "PAID", "to": "CONFIRMED", "reason": "payment captured", "event_id": "…", "created_at": "2026-01-01T12:00:03Z" }
  ]
}
```

`404 Not Found` if the order does not exist.

## List orders

```http
GET /v1/orders?customer_id=cust-42&status=CONFIRMED&limit=50&offset=0
```

Query parameters:

| Parameter | Type | Default | Notes |
| --- | --- | --- | --- |
| `customer_id` | string | empty | Optional filter |
| `status` | enum | empty | `PENDING`, `RESERVED`, `PAID`, `CONFIRMED`, `CANCELLED` |
| `limit` | integer | 50 | Values above 200 fall back to 50 |
| `offset` | integer | 0 | Negative values fall back to 0 |

Response `200 OK`:

```json
{
  "items": [
    {
      "order_id": "0e4b9d8c-4a4e-4f2b-9a5c-5e7d8f9a0b1c",
      "customer_id": "cust-42",
      "status": "CONFIRMED",
      "currency": "EUR",
      "total_cents": 3998,
      "version": 3,
      "created_at": "2026-01-01T12:00:00Z",
      "updated_at": "2026-01-01T12:00:03Z"
    }
  ],
  "limit": 50,
  "offset": 0
}
```

List responses do not include items or history; fetch a single order for the full aggregate.

## Cancel an order

```http
POST /v1/orders/{id}/cancel
Content-Type: application/json
```

```json
{ "reason": "customer changed their mind" }
```

The body is optional. An empty reason is replaced with `customer requested cancellation`.

Behavior:

- `PENDING`, `RESERVED`, `PAID` -> `CANCELLED`, emits `order.cancelled`.
- `CANCELLED` -> `200 OK` with the existing order; no duplicate event.
- `CONFIRMED` -> `409 Conflict`; a confirmed order cannot be cancelled.
- Unknown order -> `404 Not Found`.

Response `200 OK` is the updated order.

## Health and metrics

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz` | Process liveness |
| `GET /readyz` | Dependency readiness; `503` if a dependency is down |
| `GET /metrics` | Prometheus exposition format |
| `GET /docs` | Swagger UI (gateway only) |
| `GET /openapi.yaml` | Raw OpenAPI document (gateway only) |

## Error format

All HTTP errors use:

```json
{ "error": "human readable message" }
```

| Status | When |
| --- | --- |
| `400` | Invalid JSON, validation failure, missing `Idempotency-Key` |
| `404` | Order not found |
| `409` | Idempotency request in progress, duplicate key race, invalid transition |
| `429` | Gateway rate limit exceeded |
| `502` | Gateway could not reach the order service |
| `500` | Unexpected server error |

## Rate limiting

The gateway applies a fixed-window limit per client IP using Redis:

- Default: `120` requests per minute (`RATE_LIMIT_PER_MINUTE`).
- Redis failures fail open: requests are allowed rather than taking the API down.
- Exceeding the limit returns `429 Too Many Requests`.

## Correlation

Every response includes:

- `X-Request-ID`
- `X-Correlation-ID`

Send your own `X-Correlation-ID` to tie the request to downstream event logs. If omitted, the request ID is used as the correlation ID.
