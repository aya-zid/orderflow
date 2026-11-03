// Package redis provides the shared Redis client used for HTTP idempotency
// and gateway rate limiting. Redis is never the source of truth: PostgreSQL
// constraints remain the final guard.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const (
	responsePrefix = "idem:response:"
	lockPrefix     = "idem:lock:"
)

// Client is a thin wrapper around go-redis with the helpers OrderFlow needs.
type Client struct {
	rdb *goredis.Client
}

// CachedResponse is the stored result of an idempotent HTTP request.
type CachedResponse struct {
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body"`
}

// New connects to Redis and verifies connectivity.
func New(ctx context.Context, url string) (*Client, error) {
	options, err := goredis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}

	client := goredis.NewClient(options)

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return &Client{rdb: client}, nil
}

// Close closes the underlying connection pool.
func (c *Client) Close() error {
	return c.rdb.Close()
}

// Ping verifies that Redis is reachable.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// GetResponse returns a cached idempotent response.
func (c *Client) GetResponse(ctx context.Context, key string) (CachedResponse, bool, error) {
	value, err := c.rdb.Get(ctx, responsePrefix+key).Bytes()
	if errors.Is(err, goredis.Nil) {
		return CachedResponse{}, false, nil
	}
	if err != nil {
		return CachedResponse{}, false, err
	}

	var cached CachedResponse
	if err := json.Unmarshal(value, &cached); err != nil {
		return CachedResponse{}, false, err
	}
	return cached, true, nil
}

// SaveResponse stores an idempotent response for later replays.
func (c *Client) SaveResponse(ctx context.Context, key string, status int, body []byte, ttl time.Duration) error {
	payload, err := json.Marshal(CachedResponse{Status: status, Body: body})
	if err != nil {
		return err
	}
	return c.rdb.Set(ctx, responsePrefix+key, payload, ttl).Err()
}

// TryLock acquires a short-lived idempotency lock.
func (c *Client) TryLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return c.rdb.SetNX(ctx, lockPrefix+key, "1", ttl).Result()
}

// Unlock releases the idempotency lock.
func (c *Client) Unlock(ctx context.Context, key string) error {
	return c.rdb.Del(ctx, lockPrefix+key).Err()
}

// Increment increments a counter and applies a TTL on first use.
func (c *Client) Increment(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	count, err := c.rdb.Incr(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	if count == 1 {
		if err := c.rdb.Expire(ctx, key, ttl).Err(); err != nil {
			return count, err
		}
	}
	return count, nil
}
