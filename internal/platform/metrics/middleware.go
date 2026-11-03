package metrics

import (
	"strconv"
	"time"

	"github.com/labstack/echo/v4"
)

// Middleware records request count and latency for every HTTP route.
func Middleware(m *Metrics) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			start := time.Now()
			err := next(c)
			if err != nil {
				c.Error(err)
			}

			route := c.Path()
			if route == "" {
				route = "unmatched"
			}
			status := strconv.Itoa(c.Response().Status)

			m.HTTPRequests.WithLabelValues(c.Request().Method, route, status).Inc()
			m.HTTPDuration.WithLabelValues(c.Request().Method, route).Observe(time.Since(start).Seconds())

			return nil
		}
	}
}
