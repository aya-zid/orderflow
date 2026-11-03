package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/aya-zid/orderflow/internal/gateway"
	"github.com/aya-zid/orderflow/internal/platform/config"
	"github.com/aya-zid/orderflow/internal/platform/httpserver"
	"github.com/aya-zid/orderflow/internal/platform/logger"
	"github.com/aya-zid/orderflow/internal/platform/metrics"
	platformredis "github.com/aya-zid/orderflow/internal/platform/redis"
)

func main() {
	if err := run(); err != nil {
		slog.Error("gateway stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(config.ServiceGateway)
	if err != nil {
		return err
	}

	log := logger.New(cfg.Service, cfg.Env, cfg.LogLevel)
	m := metrics.New(cfg.Service)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	redisClient, err := platformredis.New(ctx, cfg.RedisURL)
	if err != nil {
		return err
	}
	defer func() { _ = redisClient.Close() }()

	gw, err := gateway.New(cfg.OrderServiceURL, redisClient, cfg.RateLimitPerMinute, log)
	if err != nil {
		return err
	}

	e := httpserver.New(log, m)
	e.GET("/healthz", httpserver.Health)
	e.GET("/readyz", httpserver.Readiness(
		httpserver.Check{Name: "redis", Func: redisClient.Ping},
		httpserver.Check{Name: "order-service", Func: func(ctx context.Context) error {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.OrderServiceURL+"/healthz", nil)
			if err != nil {
				return err
			}
			client := &http.Client{Timeout: 2 * time.Second}
			response, err := client.Do(request)
			if err != nil {
				return err
			}
			defer func() { _ = response.Body.Close() }()
			if response.StatusCode != http.StatusOK {
				return fmt.Errorf("order service returned %d", response.StatusCode)
			}
			return nil
		}},
	))
	e.GET("/metrics", echo.WrapHandler(m.Handler()))
	gw.Register(e)

	log.Info("gateway ready", "port", cfg.HTTPPort, "order_service", cfg.OrderServiceURL)
	if err := httpserver.Run(ctx, e, cfg.HTTPPort, log); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
