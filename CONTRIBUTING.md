# Contributing

Thanks for taking a look at OrderFlow. This is primarily a portfolio project, but issues and pull requests are welcome.

## Development setup

Prerequisites:

- Go 1.23+
- Docker Engine 24+ with Compose v2

```bash
git clone https://github.com/aya-zid/orderflow.git
cd orderflow
cp .env.example .env
make tidy
make test
make up
```

The repository snapshot may not include `go.sum`; `make tidy` creates it. Commit the resulting `go.sum` with your changes.

## Branching and commits

- Branch from `main`.
- Use short, imperative commit subjects, for example `order: reject cancellation of confirmed orders`.
- Keep commits focused. Avoid mixing refactors with behavior changes.
- Do not commit secrets, `.env` files, database dumps or generated binaries.

## Tests

- Unit tests: `make test`
- Coverage: `make coverage`
- Integration tests (stack must be running): `make integration`
- Lint: `make lint`

New behavior should have a test. State-machine changes need table-driven tests in `internal/order/domain`. Repository changes should be covered by integration tests or a documented manual check.

## Event and API changes

- Treat the event envelope and payloads as a public contract.
- Additive payload fields are allowed without a version bump.
- Breaking changes need a new event version or a new event type with a migration plan.
- Update `api/openapi.yaml` and `docs/API.md` when an HTTP endpoint changes.
- Update `docs/EVENT_FLOW.md` when routing keys, queues or event semantics change.

## Migrations

- Never edit an applied migration; add a new `*.up.sql` file.
- Keep migrations additive and backward compatible.
- The migration runner applies files in lexical order inside a transaction.

## Pull requests

- Fill out the PR template.
- Link the issue if one exists.
- Include a short test plan.
- Call out anything intentionally out of scope.

## Code style

- Follow standard Go conventions.
- Keep packages small and dependencies pointing inward (domain does not import infrastructure).
- Use `slog` for structured logs and include `correlation_id` where relevant.
- Do not add comments that restate the code; explain *why* when it is not obvious.
