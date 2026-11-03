// Package domain contains the order aggregate and its state machine. It has no
// dependency on HTTP, SQL or RabbitMQ.
package domain

import (
	"fmt"
	"strings"
	"time"
)

// Status is the lifecycle state of an order.
type Status string

const (
	StatusPending   Status = "PENDING"
	StatusReserved  Status = "RESERVED"
	StatusPaid      Status = "PAID"
	StatusConfirmed Status = "CONFIRMED"
	StatusCancelled Status = "CANCELLED"
)

var allowedTransitions = map[Status][]Status{
	StatusPending:   {StatusReserved, StatusCancelled},
	StatusReserved:  {StatusPaid, StatusCancelled},
	StatusPaid:      {StatusConfirmed, StatusCancelled},
	StatusConfirmed: {},
	StatusCancelled: {},
}

// Item is a single order line.
type Item struct {
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

// StatusChange is one entry in the order timeline.
type StatusChange struct {
	From      Status    `json:"from"`
	To        Status    `json:"to"`
	Reason    string    `json:"reason,omitempty"`
	EventID   string    `json:"event_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// Order is the aggregate root.
type Order struct {
	ID             string         `json:"order_id"`
	CustomerID     string         `json:"customer_id"`
	Status         Status         `json:"status"`
	Currency       string         `json:"currency"`
	TotalCents     int64          `json:"total_cents"`
	Items          []Item         `json:"items"`
	IdempotencyKey string         `json:"-"`
	Version        int            `json:"version"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	History        []StatusChange `json:"history,omitempty"`
}

// NewOrder validates the input and returns a PENDING order.
func NewOrder(id, customerID, currency, idempotencyKey string, items []Item) (*Order, error) {
	customerID = strings.TrimSpace(customerID)
	if customerID == "" {
		return nil, fmt.Errorf("%w: customer_id is required", ErrInvalidOrder)
	}

	currency = strings.ToUpper(strings.TrimSpace(currency))
	if len(currency) != 3 {
		return nil, fmt.Errorf("%w: currency must be a 3-letter code", ErrInvalidOrder)
	}

	if len(items) == 0 {
		return nil, fmt.Errorf("%w: at least one item is required", ErrInvalidOrder)
	}
	items = append([]Item(nil), items...)

	seen := make(map[string]struct{}, len(items))
	var total int64
	for i := range items {
		items[i].SKU = strings.TrimSpace(items[i].SKU)
		item := items[i]
		if item.SKU == "" {
			return nil, fmt.Errorf("%w: item sku is required", ErrInvalidOrder)
		}
		if item.Quantity <= 0 {
			return nil, fmt.Errorf("%w: item %s quantity must be positive", ErrInvalidOrder, item.SKU)
		}
		if item.UnitPriceCents < 0 {
			return nil, fmt.Errorf("%w: item %s price must be non-negative", ErrInvalidOrder, item.SKU)
		}
		if _, exists := seen[item.SKU]; exists {
			return nil, fmt.Errorf("%w: duplicate sku %s", ErrInvalidOrder, item.SKU)
		}
		seen[item.SKU] = struct{}{}
		total += int64(item.Quantity) * item.UnitPriceCents
	}

	now := time.Now().UTC()
	return &Order{
		ID:             id,
		CustomerID:     customerID,
		Status:         StatusPending,
		Currency:       currency,
		TotalCents:     total,
		Items:          items,
		IdempotencyKey: idempotencyKey,
		CreatedAt:      now,
		UpdatedAt:      now,
		History: []StatusChange{{
			From:      "",
			To:        StatusPending,
			CreatedAt: now,
		}},
	}, nil
}

// CanTransition reports whether moving to target is allowed.
func (o *Order) CanTransition(target Status) bool {
	if o.Status == target {
		return true
	}
	for _, allowed := range allowedTransitions[o.Status] {
		if allowed == target {
			return true
		}
	}
	return false
}

// Transition moves the order to target and records the change.
func (o *Order) Transition(target Status, reason string) error {
	if o.Status == target {
		return nil
	}
	if !o.CanTransition(target) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, o.Status, target)
	}

	from := o.Status
	o.Status = target
	o.UpdatedAt = time.Now().UTC()
	o.Version++
	o.History = append(o.History, StatusChange{
		From:      from,
		To:        target,
		Reason:    reason,
		CreatedAt: o.UpdatedAt,
	})
	return nil
}

// IsTerminal reports whether no further transitions are possible.
func (o *Order) IsTerminal() bool {
	return o.Status == StatusConfirmed || o.Status == StatusCancelled
}
