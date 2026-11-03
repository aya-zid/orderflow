package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aya-zid/orderflow/internal/order/domain"
)

func validOrder(t *testing.T) *domain.Order {
	t.Helper()
	order, err := domain.NewOrder(
		"order-1",
		"customer-1",
		"eur",
		"idem-1",
		[]domain.Item{
			{SKU: "SKU-1", Quantity: 2, UnitPriceCents: 1999},
			{SKU: "SKU-2", Quantity: 1, UnitPriceCents: 500},
		},
	)
	require.NoError(t, err)
	return order
}

func TestNewOrderCalculatesTotal(t *testing.T) {
	order := validOrder(t)

	assert.Equal(t, domain.StatusPending, order.Status)
	assert.Equal(t, "EUR", order.Currency)
	assert.Equal(t, int64(4498), order.TotalCents)
	assert.Len(t, order.History, 1)
}

func TestNewOrderValidation(t *testing.T) {
	tests := []struct {
		name string
		items []domain.Item
	}{
		{name: "no items", items: nil},
		{name: "empty sku", items: []domain.Item{{SKU: "", Quantity: 1}}},
		{name: "zero quantity", items: []domain.Item{{SKU: "SKU-1", Quantity: 0}}},
		{name: "negative price", items: []domain.Item{{SKU: "SKU-1", Quantity: 1, UnitPriceCents: -1}}},
		{
			name: "duplicate sku",
			items: []domain.Item{
				{SKU: "SKU-1", Quantity: 1},
				{SKU: "SKU-1", Quantity: 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := domain.NewOrder("order-1", "customer-1", "EUR", "idem-1", tt.items)
			assert.ErrorIs(t, err, domain.ErrInvalidOrder)
		})
	}
}

func TestTransitions(t *testing.T) {
	order := validOrder(t)

	require.NoError(t, order.Transition(domain.StatusReserved, "stock reserved"))
	require.NoError(t, order.Transition(domain.StatusPaid, "payment captured"))
	require.NoError(t, order.Transition(domain.StatusConfirmed, ""))

	assert.True(t, order.IsTerminal())
	assert.Len(t, order.History, 4)
	assert.Equal(t, 3, order.Version)
}

func TestInvalidTransition(t *testing.T) {
	order := validOrder(t)

	err := order.Transition(domain.StatusConfirmed, "skip steps")
	assert.ErrorIs(t, err, domain.ErrInvalidTransition)
}

func TestSameStatusIsIdempotent(t *testing.T) {
	order := validOrder(t)

	require.NoError(t, order.Transition(domain.StatusPending, "duplicate"))
	assert.Equal(t, 0, order.Version)
}

func TestCancellationIsTerminal(t *testing.T) {
	order := validOrder(t)

	require.NoError(t, order.Transition(domain.StatusCancelled, "customer request"))
	assert.True(t, order.IsTerminal())
	assert.ErrorIs(t, order.Transition(domain.StatusReserved, "late event"), domain.ErrInvalidTransition)
}
