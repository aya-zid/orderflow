# ADR 0001: Event-driven order processing

## Status

Accepted.

## Context

Order processing spans stock reservation, payment capture and customer
notification. Doing all of it synchronously in one request makes the API slow
and couples failures: a slow payment provider would block order creation, and a
notification outage could fail a paid order.

The project also needs to demonstrate realistic distributed-systems patterns for
a portfolio audience: asynchronous processing, at-least-once delivery, retries
and compensation.

## Decision

Use a topic exchange in RabbitMQ as the integration backbone. The order service
owns the order state machine and publishes domain events. Inventory, payment and
notification are independent consumers. Each service owns its database and never
reads another service's tables.

The synchronous HTTP API only validates and persists the order. The rest of the
flow is asynchronous.

## Consequences

Positive:

- Order creation is fast and independent of payment latency.
- Consumers can be scaled, restarted and deployed independently.
- Failures are isolated and observable as queue depth and DLQs.
- New consumers can subscribe without changing the order service.

Negative:

- The system is eventually consistent; clients must poll or subscribe.
- Debugging requires correlation IDs and event logs.
- Duplicate delivery must be handled by every consumer.
- There is no global ordering guarantee.

## Alternatives considered

- **Synchronous orchestration:** simpler to reason about, but slow and fragile.
- **Database polling by other services:** creates tight coupling to another
  service's schema.
- **Kafka:** better for replayable streams and ordering, but heavier to operate
  for this project's scope. RabbitMQ is sufficient and easy to run locally.
