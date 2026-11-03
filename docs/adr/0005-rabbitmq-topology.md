# ADR 0005: RabbitMQ retry queues and DLQs

## Status

Accepted.

## Context

Consumers encounter two kinds of failures:

- **Transient:** a database is briefly unavailable, a request times out, or a
  lock conflict aborts a transaction. Retrying later usually succeeds.
- **Permanent:** the payload is invalid, the event type is unsupported, or the
  business rule can never succeed. Retrying forever only delays the inevitable
  and blocks the queue.

A consumer that immediately requeues a failing message can also create a hot
loop that starves healthy messages.

## Decision

Use a topic exchange with one durable main queue per consumer, one retry queue
per binding, and one DLQ per consumer.

- Transient errors publish to a retry queue with `x-message-ttl` and
  `x-dead-letter-exchange`/`x-dead-letter-routing-key` back to the main
  exchange.
- `x-retry-count` is incremented on each retry.
- After `MAX_RETRIES`, the message goes to the DLQ.
- Permanent errors go directly to the DLQ.
- Invalid envelopes go directly to the DLQ.
- If publishing the retry or DLQ message fails, the original delivery is
  `Nack`ed with requeue enabled.

Names are deterministic:

- Retry: `<queue>--<routing-key>.retry`
- DLQ: `<queue>.dlq`

`cmd/replay-dlq` republishes a bounded batch, resets the retry count and sets
`x-replayed=true`. Replayed messages still pass through the inbox.

## Consequences

Positive:

- Transient failures are retried without a hot loop.
- Poison messages do not block the main queue.
- DLQs are inspectable and replayable.
- The topology is visible in the RabbitMQ management UI.

Negative:

- More queues to monitor.
- Delayed retries add latency.
- A DLQ is not a permanent archive; messages should be replayed or discarded
  deliberately.
- RabbitMQ classic queues are used locally; production should consider quorum
  queues for stronger durability.

## Alternatives considered

- **Immediate requeue:** hot loop and no backoff.
- **In-process sleep and retry:** blocks the consumer and loses state on crash.
- **No DLQ:** poison messages either loop forever or are dropped silently.
- **Kafka retry topics:** powerful, but heavier than needed for this project.
