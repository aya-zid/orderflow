# ADR 0002: Transactional outbox

## Status

Accepted.

## Context

Writing to PostgreSQL and publishing to RabbitMQ are two separate systems. A
naive implementation has a dual-write problem:

- Commit the order, then crash before publishing: the order exists but no
  service ever processes it.
- Publish the event, then the database transaction rolls back: consumers act on
  an order that does not exist.

Both failure modes are unacceptable for order processing.

## Decision

Every service that produces events writes the business change and an `outbox`
row in the same PostgreSQL transaction. A relay polls unpublished rows with
`FOR UPDATE SKIP LOCKED`, publishes them with publisher confirms, and marks
`published_at` only after the broker confirms.

The relay is per service and runs in the service process. Multiple replicas are
safe because `SKIP LOCKED` prevents overlapping batches.

## Consequences

Positive:

- No committed business change can be lost.
- No event can be published for a rolled-back change.
- The outbox is a durable audit trail.
- The pattern is independent of the broker client.

Negative:

- Extra table and relay process.
- Publishing is delayed by up to one polling interval.
- If the process crashes after publish and before marking, the event is
  published again. Consumers must be idempotent.
- Outbox rows need periodic cleanup in a long-running system.

## Alternatives considered

- **Publish inside the transaction:** impossible; the broker is not part of the
  database transaction.
- **Two-phase commit:** not supported by RabbitMQ and operationally expensive.
- **Change data capture (Debezium):** robust, but adds Kafka Connect and a much
  larger operational footprint than this project needs.
