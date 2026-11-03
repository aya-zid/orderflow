// Package repository persists simulated payments and their resulting events.
package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aya-zid/orderflow/pkg/events"
)

const consumerName = "payment-service"

// Result describes the payment attempt.
type Result struct {
	Duplicate bool
	Succeeded bool
	PaymentID string
	Reason    string
}

// Repository is the PostgreSQL payment store.
type Repository struct {
	pool *pgxpool.Pool
}

// New creates a payment repository.
func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// ProcessWithOutbox charges an order. The payment row and the resulting event
// are committed together; retries are deduplicated by order_id and the inbox.
func (r *Repository) ProcessWithOutbox(
	ctx context.Context,
	env events.Envelope,
	payload events.InventoryReservedPayload,
	failureRate float64,
) (Result, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("begin payment: %w", err)
	}
	defer tx.Rollback(ctx)

	duplicate, err := markProcessed(ctx, tx, env.EventID)
	if err != nil {
		return Result{}, err
	}
	if duplicate {
		if err := tx.Commit(ctx); err != nil {
			return Result{}, err
		}
		return Result{Duplicate: true}, nil
	}

	var existingID, existingStatus, existingReason string
	err = tx.QueryRow(ctx,
		`SELECT id, status, failure_reason FROM payments WHERE order_id = $1`,
		payload.OrderID,
	).Scan(&existingID, &existingStatus, &existingReason)
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return Result{}, err
		}
		return Result{
			Duplicate: true,
			Succeeded: existingStatus == "SUCCEEDED",
			PaymentID: existingID,
			Reason:    existingReason,
		}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Result{}, fmt.Errorf("check existing payment: %w", err)
	}

	paymentID := uuid.NewString()
	succeeded := rand.Float64() >= failureRate

	status := "SUCCEEDED"
	reason := ""
	if !succeeded {
		status = "FAILED"
		reason = "simulated card decline"
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO payments (id, order_id, amount_cents, currency, status, failure_reason)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		paymentID, payload.OrderID, payload.TotalCents, payload.Currency, status, reason,
	); err != nil {
		return Result{}, fmt.Errorf("insert payment: %w", err)
	}

	var out events.Envelope
	if succeeded {
		out, err = events.New(
			events.EventPaymentSucceeded,
			events.ProducerPayment,
			env.CorrelationID,
			env.EventID,
			events.PaymentSucceededPayload{OrderID: payload.OrderID, PaymentID: paymentID},
		)
	} else {
		out, err = events.New(
			events.EventPaymentFailed,
			events.ProducerPayment,
			env.CorrelationID,
			env.EventID,
			events.PaymentFailedPayload{OrderID: payload.OrderID, Reason: reason},
		)
	}
	if err != nil {
		return Result{}, err
	}

	if err := insertOutbox(ctx, tx, payload.OrderID, out); err != nil {
		return Result{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Result{}, fmt.Errorf("commit payment: %w", err)
	}
	return Result{Succeeded: succeeded, PaymentID: paymentID, Reason: reason}, nil
}

func markProcessed(ctx context.Context, tx pgx.Tx, eventID string) (bool, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO processed_events (consumer, event_id)
		VALUES ($1, $2)
		ON CONFLICT DO NOTHING`, consumerName, eventID)
	if err != nil {
		return false, fmt.Errorf("insert inbox row: %w", err)
	}
	return tag.RowsAffected() == 0, nil
}

func insertOutbox(ctx context.Context, tx pgx.Tx, aggregateID string, env events.Envelope) error {
	payload, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal outbox event: %w", err)
	}
	headers, err := json.Marshal(map[string]string{
		"correlation_id": env.CorrelationID,
		"causation_id":   env.CausationID,
		"producer":       env.Producer,
	})
	if err != nil {
		return fmt.Errorf("marshal outbox headers: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox (id, aggregate_id, event_type, payload, headers, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		env.EventID, aggregateID, env.EventType, string(payload), string(headers), env.OccurredAt,
	); err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}
