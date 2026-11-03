// Package app contains the payment use case.
package app

import (
	"context"
	"log/slog"

	"github.com/aya-zid/orderflow/internal/payment/repository"
	"github.com/aya-zid/orderflow/pkg/events"
)

// Repository is the payment persistence port.
type Repository interface {
	ProcessWithOutbox(ctx context.Context, env events.Envelope, payload events.InventoryReservedPayload, failureRate float64) (repository.Result, error)
}

// Service simulates payment capture for reserved orders.
type Service struct {
	repo        Repository
	failureRate float64
	log         *slog.Logger
}

// New creates the payment service.
func New(repo Repository, failureRate float64, log *slog.Logger) *Service {
	return &Service{repo: repo, failureRate: failureRate, log: log}
}

// HandleInventoryReserved attempts to charge the order.
func (s *Service) HandleInventoryReserved(ctx context.Context, env events.Envelope) error {
	payload, err := events.Decode[events.InventoryReservedPayload](env)
	if err != nil {
		return err
	}

	result, err := s.repo.ProcessWithOutbox(ctx, env, payload, s.failureRate)
	if err != nil {
		return err
	}
	if result.Duplicate {
		s.log.Debug("duplicate inventory.reserved ignored", "order_id", payload.OrderID)
		return nil
	}

	if result.Succeeded {
		s.log.Info("payment captured", "order_id", payload.OrderID, "payment_id", result.PaymentID)
		return nil
	}

	s.log.Warn("payment declined", "order_id", payload.OrderID, "reason", result.Reason)
	return nil
}
