// Package repository implements inventory persistence. Stock rows are locked
// and updated in the same transaction that records the inbox row and the
// outgoing event, which prevents overselling and lost events.
package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aya-zid/orderflow/internal/inventory/domain"
	"github.com/aya-zid/orderflow/pkg/events"
)

const consumerName = "inventory-service"

// Repository is the PostgreSQL inventory store.
type Repository struct {
	pool *pgxpool.Pool
}

// New creates an inventory repository.
func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// ReservationResult describes the outcome of an order.created event.
type ReservationResult struct {
	Duplicate     bool
	Cancelled     bool
	Reserved      bool
	ReservationID string
	FailedSKU     string
}

// ReserveWithOutbox reserves every line of an order or rejects the whole
// order. The inbox row, stock changes, reservations and outbox event are
// committed atomically.
func (r *Repository) ReserveWithOutbox(ctx context.Context, env events.Envelope, payload events.OrderCreatedPayload) (ReservationResult, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return ReservationResult{}, fmt.Errorf("begin reservation: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		payload.OrderID,
	); err != nil {
		return ReservationResult{}, fmt.Errorf("lock order: %w", err)
	}

	duplicate, err := markProcessed(ctx, tx, env.EventID)
	if err != nil {
		return ReservationResult{}, err
	}
	if duplicate {
		if err := tx.Commit(ctx); err != nil {
			return ReservationResult{}, err
		}
		return ReservationResult{Duplicate: true}, nil
	}

	cancelled, err := isCancelled(ctx, tx, payload.OrderID)
	if err != nil {
		return ReservationResult{}, err
	}
	if cancelled {
		if err := tx.Commit(ctx); err != nil {
			return ReservationResult{}, err
		}
		return ReservationResult{Cancelled: true}, nil
	}

	failedSKU, err := lockAndCheck(ctx, tx, payload.Items)
	if err != nil {
		return ReservationResult{}, err
	}

	if failedSKU != "" {
		out, err := events.New(
			events.EventInventoryRejected,
			events.ProducerInventory,
			env.CorrelationID,
			env.EventID,
			events.InventoryRejectedPayload{
				OrderID: payload.OrderID,
				SKU:     failedSKU,
				Reason:  "insufficient stock for " + failedSKU,
			},
		)
		if err != nil {
			return ReservationResult{}, err
		}
		if err := insertOutbox(ctx, tx, payload.OrderID, out); err != nil {
			return ReservationResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return ReservationResult{}, fmt.Errorf("commit rejection: %w", err)
		}
		return ReservationResult{FailedSKU: failedSKU}, nil
	}

	groupID := uuid.NewString()
	for _, item := range payload.Items {
		tag, err := tx.Exec(ctx, `
			UPDATE inventory
			SET available = available - $1,
			    reserved  = reserved + $1,
			    version   = version + 1,
			    updated_at = now()
			WHERE sku = $2 AND available >= $1`,
			item.Quantity, item.SKU,
		)
		if err != nil {
			return ReservationResult{}, fmt.Errorf("reserve %s: %w", item.SKU, err)
		}
		if tag.RowsAffected() != 1 {
			return ReservationResult{}, fmt.Errorf("%w: %s", domain.ErrInsufficientStock, item.SKU)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO reservations (group_id, order_id, sku, quantity, status)
			VALUES ($1, $2, $3, $4, $5)`,
			groupID, payload.OrderID, item.SKU, item.Quantity, string(domain.ReservationReserved),
		); err != nil {
			return ReservationResult{}, fmt.Errorf("insert reservation: %w", err)
		}
	}

	out, err := events.New(
		events.EventInventoryReserved,
		events.ProducerInventory,
		env.CorrelationID,
		env.EventID,
		events.InventoryReservedPayload{
			OrderID:       payload.OrderID,
			ReservationID: groupID,
			TotalCents:    payload.TotalCents,
			Currency:      payload.Currency,
		},
	)
	if err != nil {
		return ReservationResult{}, err
	}
	if err := insertOutbox(ctx, tx, payload.OrderID, out); err != nil {
		return ReservationResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return ReservationResult{}, fmt.Errorf("commit reservation: %w", err)
	}
	return ReservationResult{Reserved: true, ReservationID: groupID}, nil
}

// ReleaseWithOutbox releases every RESERVED line for an order. It is safe to
// call more than once.
func (r *Repository) ReleaseWithOutbox(ctx context.Context, env events.Envelope, payload events.OrderCancelledPayload) (bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin release: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		payload.OrderID,
	); err != nil {
		return false, fmt.Errorf("lock order: %w", err)
	}

	duplicate, err := markProcessed(ctx, tx, env.EventID)
	if err != nil {
		return false, err
	}
	if duplicate {
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		return false, nil
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO cancelled_orders (order_id, reason)
		VALUES ($1, $2)
		ON CONFLICT (order_id) DO NOTHING`,
		payload.OrderID, payload.Reason,
	); err != nil {
		return false, fmt.Errorf("record cancelled order: %w", err)
	}

	type reservation struct {
		sku      string
		quantity int
	}
	rows, err := tx.Query(ctx, `
		SELECT sku, quantity
		FROM reservations
		WHERE order_id = $1 AND status = $2
		ORDER BY sku
		FOR UPDATE`,
		payload.OrderID, string(domain.ReservationReserved),
	)
	if err != nil {
		return false, fmt.Errorf("select reservations: %w", err)
	}

	reservations := make([]reservation, 0)
	for rows.Next() {
		var item reservation
		if err := rows.Scan(&item.sku, &item.quantity); err != nil {
			rows.Close()
			return false, fmt.Errorf("scan reservation: %w", err)
		}
		reservations = append(reservations, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate reservations: %w", err)
	}

	for _, item := range reservations {
		if _, err := tx.Exec(ctx, `
			UPDATE inventory
			SET available = available + $1,
			    reserved  = reserved - $1,
			    version   = version + 1,
			    updated_at = now()
			WHERE sku = $2`,
			item.quantity, item.sku,
		); err != nil {
			return false, fmt.Errorf("release %s: %w", item.sku, err)
		}
	}

	if _, err := tx.Exec(ctx, `
		UPDATE reservations
		SET status = $1
		WHERE order_id = $2 AND status = $3`,
		string(domain.ReservationReleased), payload.OrderID, string(domain.ReservationReserved),
	); err != nil {
		return false, fmt.Errorf("mark reservations released: %w", err)
	}

	if len(reservations) > 0 {
		out, err := events.New(
			events.EventInventoryReleased,
			events.ProducerInventory,
			env.CorrelationID,
			env.EventID,
			events.InventoryReleasedPayload{OrderID: payload.OrderID},
		)
		if err != nil {
			return false, err
		}
		if err := insertOutbox(ctx, tx, payload.OrderID, out); err != nil {
			return false, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit release: %w", err)
	}
	return len(reservations) > 0, nil
}

func lockAndCheck(ctx context.Context, tx pgx.Tx, items []events.OrderItem) (string, error) {
	sorted := append([]events.OrderItem(nil), items...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].SKU < sorted[j].SKU })

	for _, item := range sorted {
		var available int
		err := tx.QueryRow(ctx,
			`SELECT available FROM inventory WHERE sku = $1 FOR UPDATE`,
			item.SKU,
		).Scan(&available)
		if errors.Is(err, pgx.ErrNoRows) {
			return item.SKU, nil
		}
		if err != nil {
			return "", fmt.Errorf("lock stock %s: %w", item.SKU, err)
		}
		if available < item.Quantity {
			return item.SKU, nil
		}
	}
	return "", nil
}

func isCancelled(ctx context.Context, tx pgx.Tx, orderID string) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM cancelled_orders WHERE order_id = $1)`,
		orderID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check cancelled order: %w", err)
	}
	return exists, nil
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
