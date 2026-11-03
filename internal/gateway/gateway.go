// Package gateway exposes the public entry point. It adds request identifiers,
// rate limiting and a Swagger UI on top of the order service.
package gateway

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/aya-zid/orderflow/api"
	"github.com/aya-zid/orderflow/internal/platform/logger"
	platformredis "github.com/aya-zid/orderflow/internal/platform/redis"
)

// Gateway proxies public traffic to the order service.
type Gateway struct {
	proxy     *httputil.ReverseProxy
	redis     *platformredis.Client
	rateLimit int
	log       *slog.Logger
}

// New creates a gateway for the given order service URL.
func New(orderServiceURL string, redisClient *platformredis.Client, rateLimit int, log *slog.Logger) (*Gateway, error) {
	target, err := url.Parse(orderServiceURL)
	if err != nil {
		return nil, fmt.Errorf("parse order service url: %w", err)
	}
	if target.Scheme == "" || target.Host == "" {
		return nil, fmt.Errorf("order service url must include scheme and host")
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	originalDirector := proxy.Director
	proxy.Director = func(request *http.Request) {
		originalDirector(request)
		request.Host = target.Host
		request.Header.Set("X-Forwarded-Host", request.Host)
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Error("order service unavailable", "error", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":"order service unavailable"}`))
	}

	return &Gateway{
		proxy:     proxy,
		redis:     redisClient,
		rateLimit: rateLimit,
		log:       log,
	}, nil
}

// Register attaches gateway routes.
func (g *Gateway) Register(e *echo.Echo) {
	e.Any("/v1/*", g.handle, g.RateLimit)
	e.GET("/openapi.yaml", g.spec)
	e.GET("/docs", g.docs)
}

func (g *Gateway) handle(c echo.Context) error {
	ctx := c.Request().Context()
	c.Request().Header.Set("X-Request-ID", logger.RequestID(ctx))
	c.Request().Header.Set("X-Correlation-ID", logger.CorrelationID(ctx))
	g.proxy.ServeHTTP(c.Response(), c.Request())
	return nil
}

// RateLimit applies a fixed window per IP using Redis. Redis failures fail
// open so the gateway never becomes a single point of failure.
func (g *Gateway) RateLimit(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		if g.redis == nil || g.rateLimit <= 0 {
			return next(c)
		}

		ip := c.RealIP()
		window := time.Now().Unix() / 60
		key := fmt.Sprintf("ratelimit:%s:%d", ip, window)

		count, err := g.redis.Increment(c.Request().Context(), key, time.Minute)
		if err != nil {
			g.log.Warn("rate limit unavailable", "error", err)
			return next(c)
		}
		if count > int64(g.rateLimit) {
			return echo.NewHTTPError(http.StatusTooManyRequests, "rate limit exceeded")
		}
		return next(c)
	}
}

func (g *Gateway) spec(c echo.Context) error {
	return c.Blob(http.StatusOK, "application/yaml", api.OpenAPISpec)
}

func (g *Gateway) docs(c echo.Context) error {
	return c.HTML(http.StatusOK, strings.TrimSpace(swaggerHTML))
}

const swaggerHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>OrderFlow API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
  <style>body { margin: 0; background: #fafafa; }</style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = () => {
      window.ui = SwaggerUIBundle({
        url: "/openapi.yaml",
        dom_id: "#swagger-ui",
        deepLinking: true,
        displayRequestDuration: true
      });
    };
  </script>
</body>
</html>`
