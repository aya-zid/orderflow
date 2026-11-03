// Package postgres creates connection pools and applies embedded SQL
// migrations. Each service owns its own logical database.
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// New creates a tuned pgx pool and verifies connectivity.
func New(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database dsn: %w", err)
	}

	poolConfig.MaxConns = 20
	poolConfig.MinConns = 2
	poolConfig.MaxConnLifetime = time.Hour
	poolConfig.MaxConnIdleTime = 15 * time.Minute
	poolConfig.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return pool, nil
}
