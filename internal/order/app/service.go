// Package app contains the order use cases. It depends on domain ports, not on
// concrete infrastructure.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/aya-zid/orderflow/internal/order/domain"
	"github.com/aya-zid/orderflow/internal/platform/logger"
	"github.com/aya-zid/orderflow/internal/platform/metrics"
	"github.com/aya-zid/orderflow/pkg/events"
)

// Repository is the persistence port used by the order use cases.
type Repository interface {
	CreateWithOutbox(ctx context.Context, order *domain.Order, env events.Envelope) error
	GetByID(ctx context.Context, id string) (*domain.Order, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*domain.Order, error)
	List(ctx context.Context, customerID string, status domain.Status, limit, offset int) ([]*domain.Order, error)
	UpdateStatusWithOutbox(ctx context.Context, id string, target domain.Status, reason string, env *events.Envelope) (*domain.Order, error)
	ApplyEvent(ctx context.Context, consumer string, env events.Envelope, targets []domain.Status, reason string, outboxEnv *events.Envelope) (*domain.Order, error)
}

// Service implements order use cases and event handlers.
type Service struct {
	repo    Repository
	log     *slog.Logger
	metrics *metrics.Metrics
}

// New creates the order service.
func New(repo Repository, log *slog.Logger, m *metrics.Metrics) *Service {
	return &Service{repo: repo, log: log, metrics: m}
}

// CreateInput is the validated input for creating an order.
type CreateInput struct {
	CustomerID     string
	Currency       string
	IdempotencyKey string
	Items          []domain.Item
}

// Create accepts an order, writes it and its outbox event atomically, and
// returns the existing order when the idempotency key was already used.
func (s *Service) Create(ctx context.Context, input CreateInput) (*domain.Order, error) {
	if input.IdempotencyKey == "" {
		return nil, fmt.Errorf("%w: Idempotency-Key header is required", domain.ErrInvalidOrder)
	}

	if existing, err := s.repo.GetByIdempotencyKey(ctx, input.IdempotencyKey); err == nil {
		return existing, nil
	} else if !errors.Is(err, domain.ErrNotFound) {
		return nil, err
	}

	order, err := domain.NewOrder(
		uuid.NewString(),
		input.CustomerID,
		input.Currency,
		input.IdempotencyKey,
		input.Items,
	)
	if err != nil {
		return nil, err
	}

	payload := events.OrderCreatedPayload{
		OrderID:    order.ID,
		CustomerID: order.CustomerID,
		Currency:   order.Currency,
		TotalCents: order.TotalCents,
	}
	for _, item := range order.Items {
		payload.Items = append(payload.Items, events.OrderItem{
			SKU:            item.SKU,
			Quantity:       item.Quantity,
			UnitPriceCents: item.UnitPriceCents,
		})
	}

	env, err := events.New(
		events.EventOrderCreated,
		events.ProducerOrder,
		logger.CorrelationID(ctx),
		"",
		payload,
	)
	if err != nil {
		return nil, err
	}

	if err := s.repo.CreateWithOutbox(ctx, order, env); err != nil {
		if errors.Is(err, domain.ErrConflict) {
			if existing, getErr := s.repo.GetByIdempotencyKey(ctx, input.IdempotencyKey); getErr == nil {
				return existing, nil
			}
		}
		return nil, err
	}

	s.metrics.OrdersCreated.Inc()
	s.log.Info("order created", "order_id", order.ID, "customer_id", order.CustomerID)
	return order, nil
}

// Get returns an order by id.
func (s *Service) Get(ctx context.Context, id string) (*domain.Order, error) {
	return s.repo.GetByID(ctx, id)
}

// List returns orders filtered by customer and status.
func (s *Service) List(ctx context.Context, customerID string, status domain.Status, limit, offset int) ([]*domain.Order, error) {
	return s.repo.List(ctx, customerID, status, limit, offset)
}

// Cancel cancels an order and emits order.cancelled.
func (s *Service) Cancel(ctx context.Context, id, reason string) (*domain.Order, error) {
	order, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if order.Status == domain.StatusCancelled {
		return order, nil
	}
	if order.IsTerminal() {
		return nil, fmt.Errorf("%w: cannot cancel a %s order", domain.ErrInvalidTransition, order.Status)
	}

	if reason == "" {
		reason = "customer requested cancellation"
	}

	env, err := events.New(
		events.EventOrderCancelled,
		events.ProducerOrder,
		logger.CorrelationID(ctx),
		"",
		events.OrderCancelledPayload{OrderID: id, CustomerID: order.CustomerID, Reason: reason},
	)
	if err != nil {
		return nil, err
	}

	updated, err := s.repo.UpdateStatusWithOutbox(ctx, id, domain.StatusCancelled, reason, &env)
	if err != nil {
		return nil, err
	}
	s.metrics.OrdersCancelled.WithLabelValues("customer").Inc()
	s.log.Info("order cancelled", "order_id", id, "reason", reason)
	return updated, nil
}

// HandleInventoryReserved moves an order to RESERVED.
func (s *Service) HandleInventoryReserved(ctx context.Context, env events.Envelope) error {
	payload, err := events.Decode[events.InventoryReservedPayload](env)
	if err != nil {
		return err
	}
	if payload.OrderID == "" {
		return errors.New("inventory.reserved missing order_id")
	}

	order, err := s.repo.ApplyEvent(ctx, "order-service", env, []domain.Status{domain.StatusReserved}, "inventory reserved", nil)
	if err != nil {
		return ignoreStale(s.log, env, err)
	}
	s.log.Info("order reserved", "order_id", order.ID, "reservation_id", payload.ReservationID)
	return nil
}

// HandleInventoryRejected cancels an order when stock cannot be reserved.
func (s *Service) HandleInventoryRejected(ctx context.Context, env events.Envelope) error {
	payload, err := events.Decode[events.InventoryRejectedPayload](env)
	if err != nil {
		return err
	}

	reason := payload.Reason
	if reason == "" {
		reason = "inventory rejected"
	}

	out, err := events.New(
		events.EventOrderCancelled,
		events.ProducerOrder,
		env.CorrelationID,
		env.EventID,
		events.OrderCancelledPayload{OrderID: payload.OrderID, Reason: reason},
	)
	if err != nil {
		return err
	}

	order, err := s.repo.ApplyEvent(ctx, "order-service", env, []domain.Status{domain.StatusCancelled}, reason, &out)
	if err != nil {
		return ignoreStale(s.log, env, err)
	}
	if transitionApplied(order, env.EventID, domain.StatusCancelled) {
		s.metrics.OrdersCancelled.WithLabelValues("inventory").Inc()
	}
	s.log.Info("order cancelled after inventory rejection", "order_id", payload.OrderID, "reason", reason)
	return nil
}

// HandlePaymentSucceeded confirms the order and emits order.confirmed.
func (s *Service) HandlePaymentSucceeded(ctx context.Context, env events.Envelope) error {
	payload, err := events.Decode[events.PaymentSucceededPayload](env)
	if err != nil {
		return err
	}

	order, err := s.repo.GetByID(ctx, payload.OrderID)
	if err != nil {
		return err
	}

	out, err := events.New(
		events.EventOrderConfirmed,
		events.ProducerOrder,
		env.CorrelationID,
		env.EventID,
		events.OrderConfirmedPayload{OrderID: order.ID, CustomerID: order.CustomerID},
	)
	if err != nil {
		return err
	}

	updated, err := s.repo.ApplyEvent(
		ctx,
		"order-service",
		env,
		[]domain.Status{domain.StatusPaid, domain.StatusConfirmed},
		"payment captured",
		&out,
	)
	if err != nil {
		return ignoreStale(s.log, env, err)
	}
	if transitionApplied(updated, env.EventID, domain.StatusConfirmed) {
		s.metrics.OrdersConfirmed.Inc()
	}
	s.log.Info("order confirmed", "order_id", payload.OrderID, "payment_id", payload.PaymentID)
	return nil
}

// HandlePaymentFailed cancels the order so inventory can release the stock.
func (s *Service) HandlePaymentFailed(ctx context.Context, env events.Envelope) error {
	payload, err := events.Decode[events.PaymentFailedPayload](env)
	if err != nil {
		return err
	}

	reason := payload.Reason
	if reason == "" {
		reason = "payment failed"
	}

	out, err := events.New(
		events.EventOrderCancelled,
		events.ProducerOrder,
		env.CorrelationID,
		env.EventID,
		events.OrderCancelledPayload{OrderID: payload.OrderID, Reason: reason},
	)
	if err != nil {
		return err
	}

	order, err := s.repo.ApplyEvent(ctx, "order-service", env, []domain.Status{domain.StatusCancelled}, reason, &out)
	if err != nil {
		return ignoreStale(s.log, env, err)
	}
	if transitionApplied(order, env.EventID, domain.StatusCancelled) {
		s.metrics.OrdersCancelled.WithLabelValues("payment").Inc()
	}
	s.log.Info("order cancelled after payment failure", "order_id", payload.OrderID, "reason", reason)
	return nil
}

func ignoreStale(log *slog.Logger, env events.Envelope, err error) error {
	if errors.Is(err, domain.ErrStaleEvent) {
		log.Warn("ignoring stale event", "event_type", env.EventType, "event_id", env.EventID)
		return nil
	}
	return err
}

func transitionApplied(order *domain.Order, eventID string, target domain.Status) bool {
	for _, change := range order.History {
		if change.EventID == eventID && change.To == target {
			return true
		}
	}
	return false
}
