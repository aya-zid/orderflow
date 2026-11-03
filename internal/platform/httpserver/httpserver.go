// Package httpserver wires Echo with consistent middleware, error handling
// and graceful shutdown for every service.
package httpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"github.com/aya-zid/orderflow/internal/platform/logger"
	"github.com/aya-zid/orderflow/internal/platform/metrics"
)

// New returns a configured Echo instance.
func New(log *slog.Logger, m *metrics.Metrics) *echo.Echo {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.Use(middleware.Recover())
	if m != nil {
		e.Use(metrics.Middleware(m))
	}
	e.Use(RequestContext(log))

	e.HTTPErrorHandler = ErrorHandler(log)
	return e
}

// RequestContext assigns request/correlation identifiers and logs completion.
func RequestContext(log *slog.Logger) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			requestID := c.Request().Header.Get("X-Request-ID")
			if requestID == "" {
				requestID = uuid.NewString()
			}

			correlationID := c.Request().Header.Get("X-Correlation-ID")
			if correlationID == "" {
				correlationID = requestID
			}

			ctx := logger.WithRequestID(c.Request().Context(), requestID)
			ctx = logger.WithCorrelationID(ctx, correlationID)
			c.SetRequest(c.Request().WithContext(ctx))

			c.Response().Header().Set("X-Request-ID", requestID)
			c.Response().Header().Set("X-Correlation-ID", correlationID)

			start := time.Now()
			err := next(c)
			if err != nil {
				c.Error(err)
			}

			logger.FromContext(ctx, log).Info("http request",
				"method", c.Request().Method,
				"path", c.Request().URL.Path,
				"status", c.Response().Status,
				"duration_ms", time.Since(start).Milliseconds(),
			)

			return nil
		}
	}
}

// ErrorHandler maps errors to a stable JSON error body.
func ErrorHandler(log *slog.Logger) echo.HTTPErrorHandler {
	return func(err error, c echo.Context) {
		if c.Response().Committed {
			return
		}

		status := http.StatusInternalServerError
		message := http.StatusText(status)

		var httpErr *echo.HTTPError
		if errors.As(err, &httpErr) {
			status = httpErr.Code
			message = fmt.Sprint(httpErr.Message)
		}

		logger.FromContext(c.Request().Context(), log).Error("request failed", "error", err)
		_ = c.JSON(status, map[string]string{"error": message})
	}
}

// Run starts the server and shuts it down when ctx is cancelled.
func Run(ctx context.Context, e *echo.Echo, port int, log *slog.Logger) error {
	errCh := make(chan error, 1)

	go func() {
		log.Info("http server listening", "port", port)
		if err := e.Start(fmt.Sprintf(":%d", port)); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return e.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

// Health returns a simple liveness handler.
func Health(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
}

// Check is a named readiness dependency.
type Check struct {
	Name string
	Func func(context.Context) error
}

// Readiness returns a handler that reports the first failing dependency.
func Readiness(checks ...Check) echo.HandlerFunc {
	return func(c echo.Context) error {
		for _, check := range checks {
			if err := check.Func(c.Request().Context()); err != nil {
				return c.JSON(http.StatusServiceUnavailable, map[string]string{
					"status":     "not ready",
					"dependency": check.Name,
					"error":      err.Error(),
				})
			}
		}
		return c.JSON(http.StatusOK, map[string]string{"status": "ready"})
	}
}
