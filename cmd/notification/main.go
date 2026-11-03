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

	"github.com/aya-zid/orderflow/internal/notification/app"
	"github.com/aya-zid/orderflow/internal/notification/migrations"
	"github.com/aya-zid/orderflow/internal/notification/repository"
	"github.com/aya-zid/orderflow/internal/platform/config"
	"github.com/aya-zid/orderflow/internal/platform/httpserver"
	"github.com/aya-zid/orderflow/internal/platform/logger"
	"github.com/aya-zid/orderflow/internal/platform/metrics"
	"github.com/aya-zid/orderflow/internal/platform/postgres"
	"github.com/aya-zid/orderflow/internal/platform/rabbitmq"
	"github.com/aya-zid/orderflow/pkg/events"
)

func main() {
	if err := run(); err != nil {
		slog.Error("notification service stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(config.ServiceNotification)
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

	repo := repository.New(pool)
	service := app.New(repo, log)

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

	var wg sync.WaitGroup

	consumer, err := rabbitmq.NewConsumer(
		conn,
		cfg.Exchange,
		"notification.order-events",
		[]string{events.EventOrderConfirmed, events.EventOrderCancelled},
		cfg.RetryTTL,
		cfg.MaxRetries,
		func(ctx context.Context, env events.Envelope) error {
			switch env.EventType {
			case events.EventOrderConfirmed:
				return service.HandleOrderConfirmed(ctx, env)
			case events.EventOrderCancelled:
				return service.HandleOrderCancelled(ctx, env)
			default:
				return rabbitmq.Permanent(fmt.Errorf("unsupported event %s", env.EventType))
			}
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

	log.Info("notification service ready", "port", cfg.HTTPPort)
	if err := httpserver.Run(ctx, e, cfg.HTTPPort, log); err != nil {
		return err
	}

	wg.Wait()
	return nil
}
