// Package logger configures structured JSON logging and propagates request
// metadata through context.
package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type ctxKey string

const (
	requestIDKey     ctxKey = "request_id"
	correlationIDKey ctxKey = "correlation_id"
	orderIDKey       ctxKey = "order_id"
)

// New returns a JSON logger tagged with the service name and environment.
func New(service, env, level string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLevel(level),
	})
	return slog.New(handler).With("service", service, "env", env)
}

// FromContext enriches base with the request identifiers stored in ctx.
func FromContext(ctx context.Context, base *slog.Logger) *slog.Logger {
	log := base
	if id := RequestID(ctx); id != "" {
		log = log.With("request_id", id)
	}
	if id := CorrelationID(ctx); id != "" {
		log = log.With("correlation_id", id)
	}
	if id := OrderID(ctx); id != "" {
		log = log.With("order_id", id)
	}
	return log
}

func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

func RequestID(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey).(string)
	return value
}

func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey, id)
}

func CorrelationID(ctx context.Context) string {
	value, _ := ctx.Value(correlationIDKey).(string)
	return value
}

func WithOrderID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, orderIDKey, id)
}

func OrderID(ctx context.Context) string {
	value, _ := ctx.Value(orderIDKey).(string)
	return value
}

func parseLevel(value string) slog.Level {
	switch strings.ToLower(value) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
