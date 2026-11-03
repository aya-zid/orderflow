package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aya-zid/orderflow/internal/platform/config"
)

func TestLoadOrderService(t *testing.T) {
	t.Setenv("DB_URL", "postgres://orderflow:orderflow@localhost:5432/order_db?sslmode=disable")
	t.Setenv("HTTP_PORT", "8081")
	t.Setenv("RETRY_TTL", "2s")

	cfg, err := config.Load(config.ServiceOrder)
	require.NoError(t, err)

	assert.Equal(t, config.ServiceOrder, cfg.Service)
	assert.Equal(t, 8081, cfg.HTTPPort)
	assert.Equal(t, 2*time.Second, cfg.RetryTTL)
	assert.True(t, cfg.RequiresDatabase())
}

func TestLoadRequiresDatabaseURL(t *testing.T) {
	t.Setenv("DB_URL", "")

	_, err := config.Load(config.ServiceOrder)
	assert.Error(t, err)
}

func TestLoadRejectsInvalidFailureRate(t *testing.T) {
	t.Setenv("DB_URL", "postgres://localhost/order_db")
	t.Setenv("PAYMENT_FAILURE_RATE", "1.5")

	_, err := config.Load(config.ServicePayment)
	assert.Error(t, err)
}

func TestGatewayDoesNotRequireDatabase(t *testing.T) {
	t.Setenv("DB_URL", "")

	cfg, err := config.Load(config.ServiceGateway)
	require.NoError(t, err)
	assert.False(t, cfg.RequiresDatabase())
	assert.Equal(t, 8080, cfg.HTTPPort)
}
