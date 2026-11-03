# syntax=docker/dockerfile:1

FROM golang:1.23-alpine AS builder

ARG SERVICE
RUN apk add --no-cache git ca-certificates

WORKDIR /src

COPY go.mod ./
COPY . .
RUN go mod tidy

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/service \
    ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot

ARG SERVICE
LABEL org.opencontainers.image.title="orderflow-${SERVICE}" \
      org.opencontainers.image.description="OrderFlow ${SERVICE} service" \
      org.opencontainers.image.source="https://github.com/aya-zid/orderflow"

COPY --from=builder /out/service /service

USER nonroot:nonroot
ENTRYPOINT ["/service"]
