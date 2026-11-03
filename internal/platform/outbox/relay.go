// Package outbox implements the transactional outbox relay.
//
// Business writes and outbox rows are committed in the same database
// transaction. The relay then publishes pending rows and marks them as sent.
// This removes the dual-write failure mode where a committed order has no
// corresponding event.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aya-zid/orderflow/internal/platform/metrics"
	"github.com/aya-zid/orderflow/pkg/events"
)

// Publisher is the subset of the RabbitMQ publisher the relay needs.
type Publisher interface {
	Publish(ctx context.Context, env events.Envelope) error
}

// Relay polls the outbox table and publishes pending events.
type Relay struct {
	pool      *pgxpool.Pool
	publisher Publisher
	interval  time.Duration
	batchSize int
	metrics   *metrics.Metrics
	log       *slog.Logger
}

// New creates an outbox relay.
func New(pool *pgxpool.Pool, publisher Publisher, interval time.Duration, m *metrics.Metrics, log *slog.Logger) *Relay {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	return &Relay{
		pool:      pool,
		publisher: publisher,
		interval:  interval,
		batchSize: 100,
		metrics:   m,
		log:       log,
	}
}

// Run polls until the context is cancelled.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.log.Info("outbox relay started", "interval", r.interval)

	for {
		select {
		case <-ctx.Done():
			r.log.Info("outbox relay stopped")
			return
		case <-ticker.C:
			if err := r.publishBatch(ctx); err != nil {
				r.log.Error("outbox batch failed", "error", err)
			}
			r.refreshPending(ctx)
		}
	}
}

func (r *Relay) publishBatch(ctx context.Context) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin outbox tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT id, payload
		FROM outbox
		WHERE published_at IS NULL
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, r.batchSize)
	if err != nil {
		return fmt.Errorf("select outbox rows: %w", err)
	}

	type record struct {
		id      string
		payload []byte
	}
	records := make([]record, 0, r.batchSize)

	for rows.Next() {
		var item record
		if err := rows.Scan(&item.id, &item.payload); err != nil {
			rows.Close()
			return fmt.Errorf("scan outbox row: %w", err)
		}
		records = append(records, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate outbox rows: %w", err)
	}

	for _, item := range records {
		var env events.Envelope
		if err := json.Unmarshal(item.payload, &env); err != nil {
			return fmt.Errorf("decode outbox event %s: %w", item.id, err)
		}

		if err := r.publisher.Publish(ctx, env); err != nil {
			return fmt.Errorf("publish outbox event %s: %w", item.id, err)
		}

		if _, err := tx.Exec(ctx,
			`UPDATE outbox SET published_at = now() WHERE id = $1`,
			item.id,
		); err != nil {
			return fmt.Errorf("mark outbox event %s published: %w", item.id, err)
		}

		r.metrics.EventsPublished.WithLabelValues(env.EventType).Inc()
	}

	if len(records) > 0 {
		r.log.Debug("outbox batch published", "count", len(records))
	}

	return tx.Commit(ctx)
}

func (r *Relay) refreshPending(ctx context.Context) {
	var count int64
	err := r.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE published_at IS NULL`,
	).Scan(&count)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			r.log.Warn("failed to count pending outbox rows", "error", err)
		}
		return
	}
	r.metrics.OutboxPending.WithLabelValues("outbox").Set(float64(count))
}
