// Package config loads and validates service configuration from environment
// variables. Every service uses the same loader so deployment stays uniform.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	ServiceGateway      = "gateway"
	ServiceOrder        = "order"
	ServiceInventory    = "inventory"
	ServicePayment      = "payment"
	ServiceNotification = "notification"
)

// Config is the runtime configuration shared by all OrderFlow services.
type Config struct {
	Service            string
	Env                string
	LogLevel           string
	HTTPPort           int
	DBURL              string
	RabbitMQURL        string
	RedisURL           string
	Exchange           string
	OrderServiceURL    string
	PaymentFailureRate float64
	RateLimitPerMinute int
	MaxRetries         int
	RetryTTL           time.Duration
	OutboxInterval     time.Duration
}

// Load reads configuration for the given service and validates it.
func Load(service string) (Config, error) {
	cfg := Config{
		Service:            service,
		Env:                env("ENV", "development"),
		LogLevel:           env("LOG_LEVEL", "info"),
		HTTPPort:           envInt("HTTP_PORT", defaultPort(service)),
		DBURL:              env("DB_URL", ""),
		RabbitMQURL:        env("RABBITMQ_URL", "amqp://orderflow:orderflow@localhost:5672/"),
		RedisURL:           env("REDIS_URL", "redis://localhost:6379/0"),
		Exchange:           env("RABBITMQ_EXCHANGE", "orderflow.events"),
		OrderServiceURL:    env("ORDER_SERVICE_URL", "http://localhost:8081"),
		PaymentFailureRate: envFloat("PAYMENT_FAILURE_RATE", 0.1),
		RateLimitPerMinute: envInt("RATE_LIMIT_PER_MINUTE", 120),
		MaxRetries:         envInt("MAX_RETRIES", 3),
		RetryTTL:           envDuration("RETRY_TTL", 5*time.Second),
		OutboxInterval:     envDuration("OUTBOX_INTERVAL", 500*time.Millisecond),
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate rejects obviously unsafe or incomplete configurations early.
func (c Config) Validate() error {
	switch c.Service {
	case ServiceGateway, ServiceOrder, ServiceInventory, ServicePayment, ServiceNotification:
	default:
		return fmt.Errorf("unknown service %q", c.Service)
	}

	if c.HTTPPort < 1 || c.HTTPPort > 65535 {
		return fmt.Errorf("invalid HTTP_PORT %d", c.HTTPPort)
	}
	if c.PaymentFailureRate < 0 || c.PaymentFailureRate > 1 {
		return fmt.Errorf("PAYMENT_FAILURE_RATE must be between 0 and 1")
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("MAX_RETRIES must be >= 0")
	}
	if c.RequiresDatabase() && strings.TrimSpace(c.DBURL) == "" {
		return fmt.Errorf("DB_URL is required for the %s service", c.Service)
	}
	if strings.TrimSpace(c.RabbitMQURL) == "" {
		return fmt.Errorf("RABBITMQ_URL is required")
	}
	return nil
}

// RequiresDatabase reports whether the service owns a PostgreSQL database.
func (c Config) RequiresDatabase() bool {
	return c.Service != ServiceGateway
}

// IsProduction reports whether the service runs with production defaults.
func (c Config) IsProduction() bool {
	return strings.EqualFold(c.Env, "production")
}

func defaultPort(service string) int {
	switch service {
	case ServiceGateway:
		return 8080
	case ServiceOrder:
		return 8081
	case ServiceInventory:
		return 8082
	case ServicePayment:
		return 8083
	case ServiceNotification:
		return 8084
	default:
		return 8080
	}
}

func env(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value, ok := os.LookupEnv(key)
	if !ok || value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envFloat(key string, fallback float64) float64 {
	value, ok := os.LookupEnv(key)
	if !ok || value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value, ok := os.LookupEnv(key)
	if !ok || value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
