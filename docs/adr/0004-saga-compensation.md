# ADR 0004: Saga with explicit compensation

## Status

Accepted.

## Context

An order spans inventory, payment and notification. A distributed transaction
across those services would require two-phase commit, which is unavailable with
RabbitMQ and undesirable for availability.

Failures are normal: stock can be insufficient, a payment can be declined, and a
customer can cancel while the order is still being processed. The system must
leave the data consistent even when a step fails.

## Decision

Use an orchestration-free saga coordinated by the order state machine:

- Success: `order.created` -> `inventory.reserved` -> `payment.succeeded` ->
  `order.confirmed`.
- Inventory rejection: `inventory.rejected` -> `order.cancelled`.
- Payment failure: `payment.failed` -> `order.cancelled`.
- Customer cancellation: `order.cancelled` directly.
- Compensation: inventory consumes `order.cancelled` and releases any
  reservation it holds.

Inventory records cancellation tombstones so a cancellation that arrives before
`order.created` still prevents a later reservation. The order state machine
rejects invalid transitions, and every consumer is idempotent.

## Consequences

Positive:

- No distributed transaction or coordinator process.
- Failures produce a terminal, observable state.
- Compensation is explicit and testable.
- Inventory release is idempotent.

Negative:

- Temporary inconsistency is visible to clients.
- A payment captured just before a cancellation is not automatically refunded;
  a real system needs a refund/compensation step with the payment provider.
- There is no timeout coordinator, so an order can remain non-terminal while a
  downstream service is unavailable. A reconciliation job would be needed in
  production.

## Alternatives considered

- **Two-phase commit:** not supported end-to-end and reduces availability.
- **Synchronous rollback:** impossible once another service has committed.
- **Ignore failures:** leaves stock reserved and orders stuck.
