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

	orderhttp "github.com/aya-zid/orderflow/internal/order/adapters/http"
	"github.com/aya-zid/orderflow/internal/order/app"
	"github.com/aya-zid/orderflow/internal/order/migrations"
	"github.com/aya-zid/orderflow/internal/order/repository"
	"github.com/aya-zid/orderflow/internal/platform/config"
	"github.com/aya-zid/orderflow/internal/platform/httpserver"
	"github.com/aya-zid/orderflow/internal/platform/logger"
	"github.com/aya-zid/orderflow/internal/platform/metrics"
	"github.com/aya-zid/orderflow/internal/platform/outbox"
	"github.com/aya-zid/orderflow/internal/platform/postgres"
	"github.com/aya-zid/orderflow/internal/platform/rabbitmq"
	platformredis "github.com/aya-zid/orderflow/internal/platform/redis"
	"github.com/aya-zid/orderflow/pkg/events"
)

func main() {
	if err := run(); err != nil {
		slog.Error("order service stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(config.ServiceOrder)
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

	redisClient, err := platformredis.New(ctx, cfg.RedisURL)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	repo := repository.New(pool)
	service := app.New(repo, log, m)
	handler := orderhttp.New(service, redisClient, log)

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
		httpserver.Check{Name: "redis", Func: redisClient.Ping},
	))
	e.GET("/metrics", echo.WrapHandler(m.Handler()))
	handler.Register(e)

	relay := outbox.New(pool, publisher, cfg.OutboxInterval, m, log)
	go relay.Run(ctx)

	var wg sync.WaitGroup

	inventoryConsumer, err := rabbitmq.NewConsumer(
		conn,
		cfg.Exchange,
		"order.inventory-events",
		[]string{events.EventInventoryReserved, events.EventInventoryRejected},
		cfg.RetryTTL,
		cfg.MaxRetries,
		func(ctx context.Context, env events.Envelope) error {
			switch env.EventType {
			case events.EventInventoryReserved:
				return service.HandleInventoryReserved(ctx, env)
			case events.EventInventoryRejected:
				return service.HandleInventoryRejected(ctx, env)
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
	defer inventoryConsumer.Close()

	paymentConsumer, err := rabbitmq.NewConsumer(
		conn,
		cfg.Exchange,
		"order.payment-events",
		[]string{events.EventPaymentSucceeded, events.EventPaymentFailed},
		cfg.RetryTTL,
		cfg.MaxRetries,
		func(ctx context.Context, env events.Envelope) error {
			switch env.EventType {
			case events.EventPaymentSucceeded:
				return service.HandlePaymentSucceeded(ctx, env)
			case events.EventPaymentFailed:
				return service.HandlePaymentFailed(ctx, env)
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
	defer paymentConsumer.Close()

	runConsumer := func(consumer *rabbitmq.Consumer) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("consumer stopped", "queue", "order", "error", err)
			}
		}()
	}
	runConsumer(inventoryConsumer)
	runConsumer(paymentConsumer)

	log.Info("order service ready", "port", cfg.HTTPPort)
	if err := httpserver.Run(ctx, e, cfg.HTTPPort, log); err != nil {
		return err
	}

	wg.Wait()
	return nil
}
