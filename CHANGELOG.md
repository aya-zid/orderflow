# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.0.0] - 2026-11-04

### Added

- Five Go services: gateway, order, inventory, payment and notification.
- Transactional outbox with publisher confirms and a polling relay.
- Idempotent HTTP order creation using `Idempotency-Key`, Redis caching and a PostgreSQL unique constraint.
- Idempotent consumers using a `processed_events` inbox.
- Atomic inventory reservation and release with non-negative database constraints.
- Saga compensation through `order.cancelled` and `inventory.released`.
- Cancellation tombstones to close the cancellation-before-reservation race.
- RabbitMQ topic exchange, per-binding retry queues and per-consumer DLQs.
- `replay-dlq` operational tool and a concurrent `loadgen` tool.
- Prometheus metrics and a provisioned Grafana dashboard.
- Docker Compose stack with PostgreSQL, RabbitMQ, Redis, Prometheus, Grafana and Swagger UI.
- Kustomize manifests with probes, resources, HPAs and NetworkPolicies.
- OpenAPI 3.0 contract and Swagger UI.
- Architecture, event-flow, API and operations documentation, plus five ADRs.
- Unit tests, integration tests, CI workflow and Dependabot configuration.

### Known limitations

- Payment capture is simulated; there is no real provider or refund flow.
- No authentication, TLS or secret manager.
- No reconciliation job for orders stuck in non-terminal states.
- `go.sum` is not committed in this snapshot; run `go mod tidy`.
