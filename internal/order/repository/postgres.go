// Package repository implements the order persistence port with PostgreSQL.
// Business changes and outbox rows are always written in the same transaction.
package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aya-zid/orderflow/internal/order/domain"
	"github.com/aya-zid/orderflow/pkg/events"
)

// Repository is the PostgreSQL order store.
type Repository struct {
	pool *pgxpool.Pool
}

// New creates an order repository.
func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// CreateWithOutbox inserts an order, its items, its first history row and the
// order.created event in one transaction.
func (r *Repository) CreateWithOutbox(ctx context.Context, order *domain.Order, env events.Envelope) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin create order: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = tx.Exec(ctx, `
		INSERT INTO orders
			(id, customer_id, status, currency, total_cents, idempotency_key, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		order.ID, order.CustomerID, string(order.Status), order.Currency,
		order.TotalCents, order.IdempotencyKey, order.Version, order.CreatedAt, order.UpdatedAt,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return domain.ErrConflict
		}
		return fmt.Errorf("insert order: %w", err)
	}

	for _, item := range order.Items {
		if _, err := tx.Exec(ctx, `
			INSERT INTO order_items (id, order_id, sku, quantity, unit_price_cents)
			VALUES (gen_random_uuid(), $1, $2, $3, $4)`,
			order.ID, item.SKU, item.Quantity, item.UnitPriceCents,
		); err != nil {
			return fmt.Errorf("insert order item: %w", err)
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO order_status_history (order_id, from_status, to_status, reason, event_id)
		VALUES ($1, '', $2, '', NULL)`,
		order.ID, string(order.Status),
	); err != nil {
		return fmt.Errorf("insert initial history: %w", err)
	}

	if err := insertOutbox(ctx, tx, order.ID, env); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit create order: %w", err)
	}
	return nil
}

// GetByID loads an order with its items and status timeline.
func (r *Repository) GetByID(ctx context.Context, id string) (*domain.Order, error) {
	order, err := r.loadOrder(ctx, `
		SELECT id, customer_id, status, currency, total_cents, idempotency_key, version, created_at, updated_at
		FROM orders WHERE id = $1`, id)
	if err != nil {
		return nil, err
	}
	if err := r.loadItems(ctx, order); err != nil {
		return nil, err
	}
	if err := r.loadHistory(ctx, order); err != nil {
		return nil, err
	}
	return order, nil
}

// GetByIdempotencyKey returns the order previously created with the same key.
func (r *Repository) GetByIdempotencyKey(ctx context.Context, key string) (*domain.Order, error) {
	order, err := r.loadOrder(ctx, `
		SELECT id, customer_id, status, currency, total_cents, idempotency_key, version, created_at, updated_at
		FROM orders WHERE idempotency_key = $1`, key)
	if err != nil {
		return nil, err
	}
	if err := r.loadItems(ctx, order); err != nil {
		return nil, err
	}
	if err := r.loadHistory(ctx, order); err != nil {
		return nil, err
	}
	return order, nil
}

// List returns orders filtered by customer and status.
func (r *Repository) List(ctx context.Context, customerID string, status domain.Status, limit, offset int) ([]*domain.Order, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	query := `
		SELECT id, customer_id, status, currency, total_cents, idempotency_key, version, created_at, updated_at
		FROM orders WHERE 1 = 1`
	args := make([]any, 0, 4)

	if customerID != "" {
		args = append(args, customerID)
		query += fmt.Sprintf(" AND customer_id = $%d", len(args))
	}
	if status != "" {
		args = append(args, string(status))
		query += fmt.Sprintf(" AND status = $%d", len(args))
	}

	args = append(args, limit, offset)
	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", len(args)-1, len(args))

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list orders: %w", err)
	}
	defer rows.Close()

	orders := make([]*domain.Order, 0)
	for rows.Next() {
		order := &domain.Order{}
		var statusValue string
		if err := rows.Scan(
			&order.ID, &order.CustomerID, &statusValue, &order.Currency,
			&order.TotalCents, &order.IdempotencyKey, &order.Version,
			&order.CreatedAt, &order.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan order: %w", err)
		}
		order.Status = domain.Status(statusValue)
		orders = append(orders, order)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate orders: %w", err)
	}
	return orders, nil
}

// UpdateStatusWithOutbox applies a single transition for an API-initiated
// change (for example a customer cancellation).
func (r *Repository) UpdateStatusWithOutbox(ctx context.Context, id string, target domain.Status, reason string, env *events.Envelope) (*domain.Order, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin status update: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	current, err := lockStatus(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if current != target {
		if err := applyTransition(ctx, tx, id, current, target, reason, eventID(env)); err != nil {
			return nil, err
		}
		if env != nil {
			if err := insertOutbox(ctx, tx, id, *env); err != nil {
				return nil, err
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit status update: %w", err)
	}
	return r.GetByID(ctx, id)
}

// ApplyEvent consumes an event idempotently and applies one or more
// transitions. The inbox row, status changes and optional outbox event are
// committed atomically.
func (r *Repository) ApplyEvent(
	ctx context.Context,
	consumer string,
	env events.Envelope,
	targets []domain.Status,
	reason string,
	outboxEnv *events.Envelope,
) (*domain.Order, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin apply event: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
		INSERT INTO processed_events (consumer, event_id)
		VALUES ($1, $2)
		ON CONFLICT DO NOTHING`, consumer, env.EventID)
	if err != nil {
		return nil, fmt.Errorf("insert inbox row: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return r.GetByID(ctx, env.OrderID())
	}

	orderID := env.OrderID()
	current, err := lockStatus(ctx, tx, orderID)
	if err != nil {
		return nil, err
	}

	applied := false
	for _, target := range targets {
		if current == target {
			continue
		}

		probe := &domain.Order{Status: current}
		if !probe.CanTransition(target) {
			return nil, fmt.Errorf("%w: %s -> %s", domain.ErrStaleEvent, current, target)
		}

		if err := applyTransition(ctx, tx, orderID, current, target, reason, env.EventID); err != nil {
			return nil, err
		}
		current = target
		applied = true
	}

	if applied && outboxEnv != nil {
		if err := insertOutbox(ctx, tx, orderID, *outboxEnv); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit apply event: %w", err)
	}
	return r.GetByID(ctx, orderID)
}

func (r *Repository) loadOrder(ctx context.Context, query string, args ...any) (*domain.Order, error) {
	order := &domain.Order{}
	var statusValue string

	err := r.pool.QueryRow(ctx, query, args...).Scan(
		&order.ID, &order.CustomerID, &statusValue, &order.Currency,
		&order.TotalCents, &order.IdempotencyKey, &order.Version,
		&order.CreatedAt, &order.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("load order: %w", err)
	}
	order.Status = domain.Status(statusValue)
	return order, nil
}

func (r *Repository) loadItems(ctx context.Context, order *domain.Order) error {
	rows, err := r.pool.Query(ctx, `
		SELECT sku, quantity, unit_price_cents
		FROM order_items WHERE order_id = $1 ORDER BY sku`, order.ID)
	if err != nil {
		return fmt.Errorf("load order items: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var item domain.Item
		if err := rows.Scan(&item.SKU, &item.Quantity, &item.UnitPriceCents); err != nil {
			return fmt.Errorf("scan order item: %w", err)
		}
		order.Items = append(order.Items, item)
	}
	return rows.Err()
}

func (r *Repository) loadHistory(ctx context.Context, order *domain.Order) error {
	rows, err := r.pool.Query(ctx, `
		SELECT COALESCE(from_status, ''), to_status, COALESCE(reason, ''),
		       COALESCE(event_id::text, ''), created_at
		FROM order_status_history WHERE order_id = $1 ORDER BY id`, order.ID)
	if err != nil {
		return fmt.Errorf("load order history: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var change domain.StatusChange
		var from, to string
		if err := rows.Scan(&from, &to, &change.Reason, &change.EventID, &change.CreatedAt); err != nil {
			return fmt.Errorf("scan order history: %w", err)
		}
		change.From = domain.Status(from)
		change.To = domain.Status(to)
		order.History = append(order.History, change)
	}
	return rows.Err()
}

func lockStatus(ctx context.Context, tx pgx.Tx, id string) (domain.Status, error) {
	var statusValue string
	err := tx.QueryRow(ctx, `SELECT status FROM orders WHERE id = $1 FOR UPDATE`, id).Scan(&statusValue)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", domain.ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("lock order: %w", err)
	}
	return domain.Status(statusValue), nil
}

func applyTransition(ctx context.Context, tx pgx.Tx, id string, from, to domain.Status, reason, eventID string) error {
	probe := &domain.Order{Status: from}
	if err := probe.Transition(to, reason); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE orders
		SET status = $1, version = version + 1, updated_at = now()
		WHERE id = $2`, string(to), id); err != nil {
		return fmt.Errorf("update order status: %w", err)
	}

	var event any
	if eventID != "" {
		event = eventID
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO order_status_history (order_id, from_status, to_status, reason, event_id)
		VALUES ($1, $2, $3, $4, $5)`,
		id, string(from), string(to), reason, event,
	); err != nil {
		return fmt.Errorf("insert order history: %w", err)
	}
	return nil
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

func eventID(env *events.Envelope) string {
	if env == nil {
		return ""
	}
	return env.EventID
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
