// Package domain contains the inventory entities and reservation rules.
package domain

import (
	"errors"
	"time"
)

// ReservationStatus is the lifecycle of a stock reservation.
type ReservationStatus string

const (
	ReservationReserved ReservationStatus = "RESERVED"
	ReservationReleased ReservationStatus = "RELEASED"
	ReservationRejected ReservationStatus = "REJECTED"
)

var (
	// ErrInsufficientStock is returned when a SKU cannot satisfy a request.
	ErrInsufficientStock = errors.New("insufficient stock")

	// ErrStockNotFound is returned when a SKU is not tracked.
	ErrStockNotFound = errors.New("stock not found")
)

// Stock is the available and reserved quantity for one SKU.
type Stock struct {
	SKU       string    `json:"sku"`
	Available int       `json:"available"`
	Reserved  int       `json:"reserved"`
	Version   int       `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Reservation records the stock held for one order line.
type Reservation struct {
	ID        string            `json:"reservation_id"`
	OrderID   string            `json:"order_id"`
	SKU       string            `json:"sku"`
	Quantity  int               `json:"quantity"`
	Status    ReservationStatus `json:"status"`
	CreatedAt time.Time         `json:"created_at"`
}
