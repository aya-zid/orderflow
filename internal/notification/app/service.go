// Package app contains notification use cases. Notifications are stored and
// logged; a real deployment would hand them to an email or SMS provider.
package app

import (
	"context"
	"log/slog"

	"github.com/aya-zid/orderflow/pkg/events"
)

// Repository is the notification persistence port.
type Repository interface {
	Store(ctx context.Context, env events.Envelope, orderID, notificationType, recipient string, payload any) (bool, error)
}

// Service handles final order events.
type Service struct {
	repo Repository
	log  *slog.Logger
}

// New creates the notification service.
func New(repo Repository, log *slog.Logger) *Service {
	return &Service{repo: repo, log: log}
}

// HandleOrderConfirmed sends the confirmation notification.
func (s *Service) HandleOrderConfirmed(ctx context.Context, env events.Envelope) error {
	payload, err := events.Decode[events.OrderConfirmedPayload](env)
	if err != nil {
		return err
	}

	recipient := payload.CustomerID
	if recipient == "" {
		recipient = "customer"
	}

	stored, err := s.repo.Store(ctx, env, payload.OrderID, "ORDER_CONFIRMED", recipient, map[string]string{
		"subject": "Your order is confirmed",
		"message": "Order " + payload.OrderID + " has been confirmed.",
	})
	if err != nil {
		return err
	}
	if stored {
		s.log.Info("notification sent", "order_id", payload.OrderID, "recipient", payload.CustomerID, "type", "ORDER_CONFIRMED")
	}
	return nil
}

// HandleOrderCancelled sends the cancellation notification.
func (s *Service) HandleOrderCancelled(ctx context.Context, env events.Envelope) error {
	payload, err := events.Decode[events.OrderCancelledPayload](env)
	if err != nil {
		return err
	}

	recipient := payload.CustomerID
	if recipient == "" {
		recipient = "customer"
	}

	stored, err := s.repo.Store(ctx, env, payload.OrderID, "ORDER_CANCELLED", recipient, map[string]string{
		"subject": "Your order was cancelled",
		"message": "Order " + payload.OrderID + " was cancelled: " + payload.Reason,
	})
	if err != nil {
		return err
	}
	if stored {
		s.log.Info("notification sent", "order_id", payload.OrderID, "type", "ORDER_CANCELLED")
	}
	return nil
}
