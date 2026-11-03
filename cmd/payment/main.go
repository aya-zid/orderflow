package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/labstack/echo/v4"

	"github.com/aya-zid/orderflow/internal/payment/app"
	"github.com/aya-zid/orderflow/internal/payment/migrations"
	"github.com/aya-zid/orderflow/internal/payment/repository"
	"github.com/aya-zid/orderflow/internal/platform/config"
	"github.com/aya-zid/orderflow/internal/platform/httpserver"
	"github.com/aya-zid/orderflow/internal/platform/logger"
	"github.com/aya-zid/orderflow/internal/platform/metrics"
	"github.com/aya-zid/orderflow/internal/platform/outbox"
	"github.com/aya-zid/orderflow/internal/platform/postgres"
	"github.com/aya-zid/orderflow/internal/platform/rabbitmq"
	"github.com/aya-zid/orderflow/pkg/events"
)

func main() {
	if err := run(); err != nil {
		slog.Error("payment service stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(config.ServicePayment)
	if err != nil {
		return err
	}

	log := logger.New(cfg.Service, cfg.Env, cfg.LogLevel)
	m := metrics.New(cfg.Service)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.New(ctx, cfg.DBURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := postgres.Migrate(ctx, pool, migrations.FS, "."); err != nil {
		return err
	}

	conn, err := rabbitmq.Connect(ctx, cfg.RabbitMQURL, log)
	if err != nil {
		return err
	}
	defer conn.Close()

	publisher, err := rabbitmq.NewPublisher(conn, cfg.Exchange, log)
	if err != nil {
		return err
	}
	defer publisher.Close()

	repo := repository.New(pool)
	service := app.New(repo, cfg.PaymentFailureRate, log)

	e := httpserver.New(log, m)
	e.GET("/healthz", httpserver.Health)
	e.GET("/readyz", httpserver.Readiness(
		httpserver.Check{Name: "postgres", Func: pool.Ping},
		httpserver.Check{Name: "rabbitmq", Func: func(context.Context) error {
			if conn.IsClosed() {
				return errors.New("rabbitmq connection is closed")
			}
			return nil
		}},
	))
	e.GET("/metrics", echo.WrapHandler(m.Handler()))

	relay := outbox.New(pool, publisher, cfg.OutboxInterval, m, log)
	go relay.Run(ctx)

	var wg sync.WaitGroup

	consumer, err := rabbitmq.NewConsumer(
		conn,
		cfg.Exchange,
		"payment.inventory-events",
		[]string{events.EventInventoryReserved},
		cfg.RetryTTL,
		cfg.MaxRetries,
		func(ctx context.Context, env events.Envelope) error {
			if env.EventType != events.EventInventoryReserved {
				return rabbitmq.Permanent(fmt.Errorf("unsupported event %s", env.EventType))
			}
			return service.HandleInventoryReserved(ctx, env)
		},
		m,
		log,
	)
	if err != nil {
		return err
	}
	defer consumer.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("consumer stopped", "error", err)
		}
	}()

	log.Info("payment service ready", "port", cfg.HTTPPort)
	if err := httpserver.Run(ctx, e, cfg.HTTPPort, log); err != nil {
		return err
	}

	wg.Wait()
	return nil
}
