SHELL := /bin/bash
COMPOSE := docker compose --env-file .env -f deploy/docker/docker-compose.yml
SERVICES := gateway order inventory payment notification

.PHONY: help
help:
	@echo "OrderFlow targets"
	@echo "  make tidy        Download and tidy Go modules"
	@echo "  make build       Build all services"
	@echo "  make test        Run unit tests with race detector"
	@echo "  make coverage    Run tests and write coverage.out"
	@echo "  make lint        Run go vet and golangci-lint"
	@echo "  make up          Start the full stack"
	@echo "  make down        Stop the stack and remove volumes"
	@echo "  make logs        Follow logs from all services"
	@echo "  make ps          Show container status"
	@echo "  make demo        Run the end-to-end demo (PowerShell)"
	@echo "  make loadtest    Run the concurrent order load generator"
	@echo "  make integration Run integration tests (stack must be up)"
	@echo "  make e2e         Run the concurrent end-to-end test (stack must be up)"

.PHONY: tidy
tidy:
	go mod tidy
	go mod verify

.PHONY: build
build:
	@mkdir -p bin
	@for s in $(SERVICES); do \
		echo "building $$s"; \
		CGO_ENABLED=0 go build -trimpath -o bin/$$s ./cmd/$$s || exit 1; \
	done

.PHONY: test
test:
	go test ./... -race -count=1

.PHONY: coverage
coverage:
	go test ./... -race -coverprofile=coverage.out -covermode=atomic
	go tool cover -func=coverage.out | tail -n 1

.PHONY: lint
lint:
	go vet ./...
	@command -v golangci-lint >/dev/null 2>&1 && golangci-lint run ./... || echo "golangci-lint not installed"

.PHONY: up
up:
	$(COMPOSE) up -d --build

.PHONY: down
down:
	$(COMPOSE) down -v

.PHONY: logs
logs:
	$(COMPOSE) logs -f

.PHONY: ps
ps:
	$(COMPOSE) ps

.PHONY: demo
demo:
	pwsh -File scripts/demo.ps1

.PHONY: loadtest
loadtest:
	go run ./cmd/loadgen -url http://localhost:8080 -orders 500 -concurrency 50

.PHONY: integration
integration:
	go test -tags=integration ./tests/integration/... -race -count=1

.PHONY: e2e
e2e:
	go test -tags=e2e ./tests/e2e/... -race -count=1 -v
