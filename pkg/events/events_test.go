package events_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aya-zid/orderflow/pkg/events"
)

func TestNewEnvelope(t *testing.T) {
	payload := events.OrderCreatedPayload{
		OrderID:    "order-1",
		CustomerID: "customer-1",
		Currency:   "EUR",
		Items: []events.OrderItem{
			{SKU: "SKU-1", Quantity: 2, UnitPriceCents: 1999},
		},
		TotalCents: 3998,
	}

	env, err := events.New(
		events.EventOrderCreated,
		events.ProducerOrder,
		"correlation-1",
		"causation-1",
		payload,
	)
	require.NoError(t, err)

	assert.NotEmpty(t, env.EventID)
	assert.Equal(t, events.EventOrderCreated, env.EventType)
	assert.Equal(t, events.CurrentEventVersion, env.EventVersion)
	assert.Equal(t, events.ProducerOrder, env.Producer)
	assert.Equal(t, "correlation-1", env.CorrelationID)
	assert.False(t, env.OccurredAt.IsZero())
}

func TestDecodePayload(t *testing.T) {
	want := events.OrderCancelledPayload{OrderID: "order-1", Reason: "payment failed"}
	env, err := events.New(events.EventOrderCancelled, events.ProducerOrder, "", "", want)
	require.NoError(t, err)

	got, err := events.Decode[events.OrderCancelledPayload](env)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestDecodeInvalidPayload(t *testing.T) {
	env := events.Envelope{EventType: events.EventOrderCreated, Payload: []byte(`{`)}
	_, err := events.Decode[events.OrderCreatedPayload](env)
	assert.Error(t, err)
}
