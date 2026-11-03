// Package metrics owns the Prometheus registry and the domain counters used
// by dashboards and alerts.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics is a per-process registry so tests can create isolated instances.
type Metrics struct {
	registry *prometheus.Registry

	HTTPRequests    *prometheus.CounterVec
	HTTPDuration    *prometheus.HistogramVec
	OrdersCreated   prometheus.Counter
	OrdersConfirmed prometheus.Counter
	OrdersCancelled *prometheus.CounterVec
	Reservations    *prometheus.CounterVec
	EventsPublished *prometheus.CounterVec
	EventsConsumed  *prometheus.CounterVec
	ConsumerRetries *prometheus.CounterVec
	DLQMessages     *prometheus.CounterVec
	OutboxPending   *prometheus.GaugeVec
}

// New builds and registers all collectors for a service.
func New(service string) *Metrics {
	registry := prometheus.NewRegistry()

	m := &Metrics{
		registry: registry,
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "http_requests_total",
			Help:        "Total HTTP requests.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"method", "route", "status"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "http_request_duration_seconds",
			Help:        "HTTP request latency in seconds.",
			Buckets:     prometheus.DefBuckets,
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"method", "route"}),
		OrdersCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "orders_created_total",
			Help:        "Orders accepted by the order service.",
			ConstLabels: prometheus.Labels{"service": service},
		}),
		OrdersConfirmed: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "orders_confirmed_total",
			Help:        "Orders that reached the CONFIRMED state.",
			ConstLabels: prometheus.Labels{"service": service},
		}),
		OrdersCancelled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "orders_cancelled_total",
			Help:        "Orders that reached the CANCELLED state.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"reason"}),
		Reservations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "inventory_reservations_total",
			Help:        "Inventory reservation attempts by result.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"result"}),
		EventsPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "events_published_total",
			Help:        "Events published to RabbitMQ.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"event_type"}),
		EventsConsumed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "events_consumed_total",
			Help:        "Events consumed from RabbitMQ by result.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"event_type", "result"}),
		ConsumerRetries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "consumer_retries_total",
			Help:        "Transient failures scheduled for retry.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"queue"}),
		DLQMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "dlq_messages_total",
			Help:        "Messages routed to a dead-letter queue.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"queue"}),
		OutboxPending: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "outbox_pending",
			Help:        "Unpublished transactional outbox rows.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"table"}),
	}

	registry.MustRegister(
		m.HTTPRequests,
		m.HTTPDuration,
		m.OrdersCreated,
		m.OrdersConfirmed,
		m.OrdersCancelled,
		m.Reservations,
		m.EventsPublished,
		m.EventsConsumed,
		m.ConsumerRetries,
		m.DLQMessages,
		m.OutboxPending,
	)

	return m
}

// Handler returns the Prometheus scrape handler for this registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
