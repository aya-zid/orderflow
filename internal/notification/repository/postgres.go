// Package repository stores notification records idempotently.
package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aya-zid/orderflow/pkg/events"
)

const consumerName = "notification-service"

// Repository is the PostgreSQL notification store.
type Repository struct {
	pool *pgxpool.Pool
}

// New creates a notification repository.
func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// Store records a notification exactly once for a given event.
func (r *Repository) Store(ctx context.Context, env events.Envelope, orderID, notificationType, recipient string, payload any) (bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin notification: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
		INSERT INTO processed_events (consumer, event_id)
		VALUES ($1, $2)
		ON CONFLICT DO NOTHING`, consumerName, env.EventID)
	if err != nil {
		return false, fmt.Errorf("insert inbox row: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		return false, nil
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("marshal notification payload: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO notifications (order_id, type, recipient, payload)
		VALUES ($1, $2, $3, $4)`,
		orderID, notificationType, recipient, string(body),
	); err != nil {
		return false, fmt.Errorf("insert notification: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit notification: %w", err)
	}
	return true, nil
}
