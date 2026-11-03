// Package events defines the wire format shared by every OrderFlow service.
//
// Every message published to RabbitMQ is an Envelope. Keeping the envelope
// stable and versioned lets services evolve independently without breaking
// existing consumers.
package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	EventOrderCreated        = "order.created"
	EventInventoryReserved   = "inventory.reserved"
	EventInventoryRejected   = "inventory.rejected"
	EventInventoryReleased   = "inventory.released"
	EventPaymentSucceeded    = "payment.succeeded"
	EventPaymentFailed       = "payment.failed"
	EventOrderConfirmed      = "order.confirmed"
	EventOrderCancelled      = "order.cancelled"
	CurrentEventVersion      = 1
)

const (
	ProducerOrder        = "order-service"
	ProducerInventory    = "inventory-service"
	ProducerPayment      = "payment-service"
	ProducerNotification = "notification-service"
)

// Envelope is the common message format used on the event bus.
type Envelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	EventVersion  int             `json:"event_version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Producer      string          `json:"producer"`
	CorrelationID string          `json:"correlation_id"`
	CausationID   string          `json:"causation_id"`
	Payload       json.RawMessage `json:"payload"`
}

// New builds an envelope and marshals the payload. It never mutates the input.
func New(eventType, producer, correlationID, causationID string, payload any) (Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal %s payload: %w", eventType, err)
	}

	return Envelope{
		EventID:       uuid.NewString(),
		EventType:     eventType,
		EventVersion:  CurrentEventVersion,
		OccurredAt:    time.Now().UTC(),
		Producer:      producer,
		CorrelationID: correlationID,
		CausationID:   causationID,
		Payload:       raw,
	}, nil
}

// OrderID extracts the order_id field that every OrderFlow payload carries.
func (e Envelope) OrderID() string {
	var payload struct {
		OrderID string `json:"order_id"`
	}
	_ = json.Unmarshal(e.Payload, &payload)
	return payload.OrderID
}

// Decode unmarshals an envelope payload into T.
func Decode[T any](e Envelope) (T, error) {
	var value T
	if err := json.Unmarshal(e.Payload, &value); err != nil {
		return value, fmt.Errorf("decode %s payload: %w", e.EventType, err)
	}
	return value, nil
}

// OrderItem is the immutable line item snapshot carried by order events.
type OrderItem struct {
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

type OrderCreatedPayload struct {
	OrderID     string      `json:"order_id"`
	CustomerID  string      `json:"customer_id"`
	Currency    string      `json:"currency"`
	Items       []OrderItem `json:"items"`
	TotalCents  int64       `json:"total_cents"`
}

type InventoryReservedPayload struct {
	OrderID       string `json:"order_id"`
	ReservationID string `json:"reservation_id"`
	TotalCents    int64  `json:"total_cents"`
	Currency      string `json:"currency"`
}

type InventoryRejectedPayload struct {
	OrderID string `json:"order_id"`
	SKU     string `json:"sku"`
	Reason  string `json:"reason"`
}

type InventoryReleasedPayload struct {
	OrderID string `json:"order_id"`
}

type PaymentSucceededPayload struct {
	OrderID   string `json:"order_id"`
	PaymentID string `json:"payment_id"`
}

type PaymentFailedPayload struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

type OrderConfirmedPayload struct {
	OrderID    string `json:"order_id"`
	CustomerID string `json:"customer_id"`
}

type OrderCancelledPayload struct {
	OrderID    string `json:"order_id"`
	CustomerID string `json:"customer_id,omitempty"`
	Reason     string `json:"reason"`
}
