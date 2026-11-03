# ADR 0003: Idempotent consumers with an inbox

## Status

Accepted.

## Context

The outbox relay and RabbitMQ both provide at-least-once delivery. A message can
be delivered more than once after a crash, a network partition, a retry, or a
DLQ replay. Processing it twice could reserve stock twice, charge twice, or send
duplicate notifications.

Exactly-once delivery is not available across PostgreSQL and RabbitMQ without a
distributed transaction.

## Decision

Each consumer records `(consumer, event_id)` in a `processed_events` table in
the same database transaction as its business change. The insert uses
`ON CONFLICT DO NOTHING`:

- If a row was inserted, the event is new; apply the change and commit.
- If no row was inserted, the event was already processed; commit and
  acknowledge without repeating the side effect.
- If the business change fails, the transaction rolls back, including the inbox
  row, so a retry can try again.

The inbox is per service. Two different services may process the same event.

## Consequences

Positive:

- Safe redelivery and safe DLQ replay.
- No distributed transaction or exactly-once broker feature required.
- Failures are atomic with side effects.

Negative:

- One extra write per event.
- The inbox grows forever unless pruned.
- A bug that commits the inbox row but not the side effect would suppress a
  retry; the transaction boundary prevents that if implemented correctly.

## Alternatives considered

- **Deduplicate in memory:** lost on restart; not safe.
- **Use the broker's deduplication plugin:** broker-specific and not a substitute
  for transactional side effects.
- **Make every side effect naturally idempotent:** desirable, but not sufficient
  for multi-step operations such as stock counters and payments.
