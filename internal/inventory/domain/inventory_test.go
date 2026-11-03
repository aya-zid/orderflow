package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aya-zid/orderflow/internal/inventory/domain"
)

func TestReservationStatuses(t *testing.T) {
	assert.Equal(t, "RESERVED", string(domain.ReservationReserved))
	assert.Equal(t, "RELEASED", string(domain.ReservationReleased))
	assert.Equal(t, "REJECTED", string(domain.ReservationRejected))
}
