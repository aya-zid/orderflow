package domain

import "errors"

var (
	// ErrNotFound is returned when an order does not exist.
	ErrNotFound = errors.New("order not found")

	// ErrConflict is returned when an idempotency key or order id already exists.
	ErrConflict = errors.New("order already exists")

	// ErrInvalidOrder is returned when the order payload fails validation.
	ErrInvalidOrder = errors.New("invalid order")

	// ErrInvalidTransition is returned for a disallowed state transition.
	ErrInvalidTransition = errors.New("invalid status transition")

	// ErrStaleEvent is returned when an event no longer applies to the current
	// state. Consumers acknowledge stale events instead of retrying them.
	ErrStaleEvent = errors.New("stale event")
)
