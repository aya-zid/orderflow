// Package rabbitmq wraps the AMQP client with publisher confirms, a retry
// policy and dead-letter queues. Consumers acknowledge only after the handler
// succeeds, which gives at-least-once delivery.
package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/aya-zid/orderflow/internal/platform/logger"
	"github.com/aya-zid/orderflow/internal/platform/metrics"
	"github.com/aya-zid/orderflow/pkg/events"
)

// ErrPermanent marks an error that must not be retried.
var ErrPermanent = errors.New("permanent event error")

// Permanent wraps err so the consumer sends the message straight to the DLQ.
func Permanent(err error) error {
	return fmt.Errorf("%w: %v", ErrPermanent, err)
}

func isPermanent(err error) bool {
	return errors.Is(err, ErrPermanent)
}

// Connect dials RabbitMQ with bounded exponential backoff.
func Connect(ctx context.Context, url string, log *slog.Logger) (*amqp.Connection, error) {
	var lastErr error

	for attempt := 1; attempt <= 10; attempt++ {
		conn, err := amqp.DialConfig(url, amqp.Config{
			Heartbeat: 10 * time.Second,
			Locale:    "en_US",
		})
		if err == nil {
			log.Info("connected to rabbitmq", "attempt", attempt)
			return conn, nil
		}
		lastErr = err
		log.Warn("rabbitmq connection failed", "attempt", attempt, "error", err)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(attempt) * time.Second):
		}
	}

	return nil, fmt.Errorf("connect to rabbitmq: %w", lastErr)
}

// Publisher publishes envelopes to a topic exchange with confirms enabled.
type Publisher struct {
	conn     *amqp.Connection
	ch       *amqp.Channel
	exchange string
	log      *slog.Logger
	mu       sync.Mutex
}

// NewPublisher creates a channel, declares the exchange and enables confirms.
func NewPublisher(conn *amqp.Connection, exchange string, log *slog.Logger) (*Publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open publisher channel: %w", err)
	}

	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("declare exchange %s: %w", exchange, err)
	}

	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}

	return &Publisher{conn: conn, ch: ch, exchange: exchange, log: log}, nil
}

// Publish sends an envelope and waits for the broker confirmation.
func (p *Publisher) Publish(ctx context.Context, env events.Envelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	confirmation, err := p.ch.PublishWithDeferredConfirmWithContext(
		ctx,
		p.exchange,
		env.EventType,
		false,
		false,
		amqp.Publishing{
			ContentType:   "application/json",
			DeliveryMode:  amqp.Persistent,
			MessageId:     env.EventID,
			CorrelationId: env.CorrelationID,
			Type:          env.EventType,
			Timestamp:     time.Now().UTC(),
			Headers: amqp.Table{
				"x-event-version": int32(env.EventVersion),
				"x-producer":      env.Producer,
			},
			Body: body,
		},
	)
	if err != nil {
		return fmt.Errorf("publish %s: %w", env.EventType, err)
	}
	if !confirmation.Wait() {
		return fmt.Errorf("broker rejected %s", env.EventType)
	}

	p.log.Debug("event published", "event_type", env.EventType, "event_id", env.EventID)
	return nil
}

// Close closes the publisher channel and connection.
func (p *Publisher) Close() error {
	if p.ch != nil {
		_ = p.ch.Close()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

// Handler processes a single decoded event.
type Handler func(ctx context.Context, env events.Envelope) error

// Consumer reads from a durable queue with manual acknowledgements.
type Consumer struct {
	conn        *amqp.Connection
	ch          *amqp.Channel
	exchange    string
	queue       string
	routingKeys []string
	retryTTL    time.Duration
	maxRetries  int
	handler     Handler
	metrics     *metrics.Metrics
	log         *slog.Logger
	mu          sync.Mutex
}

// NewConsumer declares the queue, bindings, retry queues and DLQ.
func NewConsumer(
	conn *amqp.Connection,
	exchange string,
	queue string,
	routingKeys []string,
	retryTTL time.Duration,
	maxRetries int,
	handler Handler,
	m *metrics.Metrics,
	log *slog.Logger,
) (*Consumer, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open consumer channel: %w", err)
	}

	if err := ch.Qos(20, 0, false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("set qos: %w", err)
	}

	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("declare exchange %s: %w", exchange, err)
	}

	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("declare queue %s: %w", queue, err)
	}

	for _, key := range routingKeys {
		if err := ch.QueueBind(queue, key, exchange, false, nil); err != nil {
			_ = ch.Close()
			return nil, fmt.Errorf("bind %s to %s: %w", key, queue, err)
		}

		retryQueue := RetryQueueName(queue, key)
		if _, err := ch.QueueDeclare(retryQueue, true, false, false, false, amqp.Table{
			"x-message-ttl":             int32(retryTTL / time.Millisecond),
			"x-dead-letter-exchange":    exchange,
			"x-dead-letter-routing-key": key,
		}); err != nil {
			_ = ch.Close()
			return nil, fmt.Errorf("declare retry queue %s: %w", retryQueue, err)
		}
	}

	dlq := DLQName(queue)
	if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("declare dlq %s: %w", dlq, err)
	}

	log.Info("consumer topology ready", "queue", queue, "routing_keys", routingKeys)

	return &Consumer{
		conn:        conn,
		ch:          ch,
		exchange:    exchange,
		queue:       queue,
		routingKeys: routingKeys,
		retryTTL:    retryTTL,
		maxRetries:  maxRetries,
		handler:     handler,
		metrics:     m,
		log:         log,
	}, nil
}

// Run blocks until the context is cancelled or the delivery channel closes.
func (c *Consumer) Run(ctx context.Context) error {
	deliveries, err := c.ch.Consume(c.queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", c.queue, err)
	}

	c.log.Info("consumer started", "queue", c.queue)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("delivery channel for %s closed", c.queue)
			}
			c.handle(ctx, delivery)
		}
	}
}

// Close closes the consumer channel.
func (c *Consumer) Close() error {
	if c.ch != nil {
		return c.ch.Close()
	}
	return nil
}

func (c *Consumer) handle(ctx context.Context, delivery amqp.Delivery) {
	log := c.log.With("routing_key", delivery.RoutingKey, "message_id", delivery.MessageId)

	var env events.Envelope
	if err := json.Unmarshal(delivery.Body, &env); err != nil {
		log.Error("invalid event envelope", "error", err)
		c.recordConsumed("unknown", "failure")
		c.toDLQ(ctx, delivery, "invalid envelope: "+err.Error())
		return
	}

	ctx = logger.WithCorrelationID(ctx, env.CorrelationID)

	if err := c.handler(ctx, env); err != nil {
		c.recordConsumed(env.EventType, "failure")
		if isPermanent(err) {
			log.Error("permanent handler error", "error", err)
			c.toDLQ(ctx, delivery, err.Error())
			return
		}
		log.Warn("transient handler error", "error", err)
		c.retryOrDLQ(ctx, delivery, err.Error())
		return
	}

	c.recordConsumed(env.EventType, "success")
	if err := delivery.Ack(false); err != nil {
		log.Error("failed to acknowledge event", "error", err)
	}
}

func (c *Consumer) recordConsumed(eventType, result string) {
	if c.metrics == nil {
		return
	}
	c.metrics.EventsConsumed.WithLabelValues(eventType, result).Inc()
}

func (c *Consumer) retryOrDLQ(ctx context.Context, delivery amqp.Delivery, cause string) {
	retries := headerInt(delivery.Headers, "x-retry-count")
	if retries >= c.maxRetries {
		c.toDLQ(ctx, delivery, cause)
		return
	}

	headers := cloneHeaders(delivery.Headers)
	headers["x-retry-count"] = int32(retries + 1)
	headers["x-last-error"] = cause

	if err := c.publish(ctx, "", RetryQueueName(c.queue, delivery.RoutingKey), delivery, headers); err != nil {
		c.log.Error("failed to schedule retry", "error", err)
		_ = delivery.Nack(false, true)
		return
	}

	if c.metrics != nil {
		c.metrics.ConsumerRetries.WithLabelValues(c.queue).Inc()
	}
	_ = delivery.Ack(false)
}

func (c *Consumer) toDLQ(ctx context.Context, delivery amqp.Delivery, reason string) {
	headers := cloneHeaders(delivery.Headers)
	headers["x-dlq-reason"] = reason

	if err := c.publish(ctx, "", DLQName(c.queue), delivery, headers); err != nil {
		c.log.Error("failed to publish to dlq", "error", err)
		_ = delivery.Nack(false, true)
		return
	}

	if c.metrics != nil {
		c.metrics.DLQMessages.WithLabelValues(c.queue).Inc()
	}
	_ = delivery.Ack(false)
}

func (c *Consumer) publish(ctx context.Context, exchange, routingKey string, delivery amqp.Delivery, headers amqp.Table) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.ch.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		ContentType:   delivery.ContentType,
		DeliveryMode:  amqp.Persistent,
		MessageId:     delivery.MessageId,
		CorrelationId: delivery.CorrelationId,
		Type:          delivery.Type,
		Headers:       headers,
		Body:          delivery.Body,
	})
}

// RetryQueueName returns the deterministic retry queue name for a binding.
func RetryQueueName(queue, routingKey string) string {
	return queue + "--" + routingKey + ".retry"
}

// DLQName returns the dead-letter queue name for a consumer queue.
func DLQName(queue string) string {
	return queue + ".dlq"
}

func cloneHeaders(headers amqp.Table) amqp.Table {
	cloned := make(amqp.Table, len(headers)+2)
	for key, value := range headers {
		cloned[key] = value
	}
	return cloned
}

func headerInt(headers amqp.Table, key string) int {
	value, ok := headers[key]
	if !ok {
		return 0
	}

	switch typed := value.(type) {
	case int:
		return typed
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	default:
		return 0
	}
}
