// Package app contains inventory use cases.
package app

import (
	"context"
	"log/slog"

	"github.com/aya-zid/orderflow/internal/inventory/repository"
	"github.com/aya-zid/orderflow/internal/platform/metrics"
	"github.com/aya-zid/orderflow/pkg/events"
)

// Repository is the inventory persistence port.
type Repository interface {
	ReserveWithOutbox(ctx context.Context, env events.Envelope, payload events.OrderCreatedPayload) (repository.ReservationResult, error)
	ReleaseWithOutbox(ctx context.Context, env events.Envelope, payload events.OrderCancelledPayload) (bool, error)
}

// Service handles inventory events.
type Service struct {
	repo    Repository
	log     *slog.Logger
	metrics *metrics.Metrics
}

// New creates the inventory service.
func New(repo Repository, log *slog.Logger, m *metrics.Metrics) *Service {
	return &Service{repo: repo, log: log, metrics: m}
}

// HandleOrderCreated reserves stock for every line of the order.
func (s *Service) HandleOrderCreated(ctx context.Context, env events.Envelope) error {
	payload, err := events.Decode[events.OrderCreatedPayload](env)
	if err != nil {
		return err
	}

	result, err := s.repo.ReserveWithOutbox(ctx, env, payload)
	if err != nil {
		return err
	}
	if result.Duplicate {
		s.log.Debug("duplicate order.created ignored", "order_id", payload.OrderID)
		return nil
	}
	if result.Cancelled {
		s.metrics.Reservations.WithLabelValues("cancelled").Inc()
		s.log.Info("order already cancelled, skipping reservation", "order_id", payload.OrderID)
		return nil
	}

	if result.Reserved {
		s.metrics.Reservations.WithLabelValues("reserved").Inc()
		s.log.Info("stock reserved",
			"order_id", payload.OrderID,
			"reservation_id", result.ReservationID,
			"items", len(payload.Items),
		)
		return nil
	}

	s.metrics.Reservations.WithLabelValues("rejected").Inc()
	s.log.Warn("stock rejected", "order_id", payload.OrderID, "sku", result.FailedSKU)
	return nil
}

// HandleOrderCancelled releases any stock still held for the order.
func (s *Service) HandleOrderCancelled(ctx context.Context, env events.Envelope) error {
	payload, err := events.Decode[events.OrderCancelledPayload](env)
	if err != nil {
		return err
	}

	released, err := s.repo.ReleaseWithOutbox(ctx, env, payload)
	if err != nil {
		return err
	}
	if released {
		s.log.Info("stock released", "order_id", payload.OrderID, "reason", payload.Reason)
	}
	return nil
}
