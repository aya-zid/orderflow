// Package http exposes the order REST API. It is the only service with a
// public write endpoint; other services are event consumers.
package http

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/aya-zid/orderflow/internal/order/app"
	"github.com/aya-zid/orderflow/internal/order/domain"
	"github.com/aya-zid/orderflow/internal/platform/logger"
	platformredis "github.com/aya-zid/orderflow/internal/platform/redis"
)

const idempotencyTTL = 24 * time.Hour

// Handler serves order endpoints.
type Handler struct {
	service *app.Service
	redis   *platformredis.Client
	log     *slog.Logger
}

// New creates an order HTTP handler.
func New(service *app.Service, redisClient *platformredis.Client, log *slog.Logger) *Handler {
	return &Handler{service: service, redis: redisClient, log: log}
}

// Register attaches order routes to the Echo instance.
func (h *Handler) Register(e *echo.Echo) {
	e.POST("/v1/orders", h.Create)
	e.GET("/v1/orders", h.List)
	e.GET("/v1/orders/:id", h.Get)
	e.POST("/v1/orders/:id/cancel", h.Cancel)
}

type createOrderRequest struct {
	CustomerID string        `json:"customer_id"`
	Currency   string        `json:"currency"`
	Items      []itemRequest `json:"items"`
}

type itemRequest struct {
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

type cancelOrderRequest struct {
	Reason string `json:"reason"`
}

// Create handles POST /v1/orders.
func (h *Handler) Create(c echo.Context) error {
	ctx := c.Request().Context()
	key := c.Request().Header.Get("Idempotency-Key")
	if key == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "Idempotency-Key header is required")
	}

	if cached, found, err := h.redis.GetResponse(ctx, key); err == nil && found {
		return c.JSONBlob(cached.Status, cached.Body)
	} else if err != nil {
		h.log.Warn("idempotency cache read failed", "error", err)
	}

	locked, err := h.redis.TryLock(ctx, key, 30*time.Second)
	if err != nil {
		h.log.Warn("idempotency lock failed, falling back to database", "error", err)
	} else if !locked {
		return echo.NewHTTPError(http.StatusConflict, "a request with this Idempotency-Key is already in progress")
	} else {
		defer func() {
			if err := h.redis.Unlock(ctx, key); err != nil {
				h.log.Warn("idempotency unlock failed", "error", err)
			}
		}()
	}

	var request createOrderRequest
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid JSON body")
	}

	items := make([]domain.Item, 0, len(request.Items))
	for _, item := range request.Items {
		items = append(items, domain.Item{
			SKU:            item.SKU,
			Quantity:       item.Quantity,
			UnitPriceCents: item.UnitPriceCents,
		})
	}

	order, err := h.service.Create(ctx, app.CreateInput{
		CustomerID:     request.CustomerID,
		Currency:       request.Currency,
		IdempotencyKey: key,
		Items:          items,
	})
	if err != nil {
		return mapError(err)
	}

	body, err := json.Marshal(order)
	if err != nil {
		return err
	}
	if err := h.redis.SaveResponse(ctx, key, http.StatusCreated, body, idempotencyTTL); err != nil {
		h.log.Warn("failed to cache idempotent response", "error", err)
	}

	logger.FromContext(ctx, h.log).Info("order created via API", "order_id", order.ID)
	return c.Blob(http.StatusCreated, echo.MIMEApplicationJSON, body)
}

// Get handles GET /v1/orders/:id.
func (h *Handler) Get(c echo.Context) error {
	order, err := h.service.Get(c.Request().Context(), c.Param("id"))
	if err != nil {
		return mapError(err)
	}
	return c.JSON(http.StatusOK, order)
}

// List handles GET /v1/orders.
func (h *Handler) List(c echo.Context) error {
	limit := parsePositiveInt(c.QueryParam("limit"), 50)
	offset := parsePositiveInt(c.QueryParam("offset"), 0)

	orders, err := h.service.List(
		c.Request().Context(),
		c.QueryParam("customer_id"),
		domain.Status(c.QueryParam("status")),
		limit,
		offset,
	)
	if err != nil {
		return mapError(err)
	}
	return c.JSON(http.StatusOK, map[string]any{
		"items":  orders,
		"limit":  limit,
		"offset": offset,
	})
}

// Cancel handles POST /v1/orders/:id/cancel.
func (h *Handler) Cancel(c echo.Context) error {
	var request cancelOrderRequest
	if err := c.Bind(&request); err != nil && !errors.Is(err, io.EOF) {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid JSON body")
	}

	order, err := h.service.Cancel(c.Request().Context(), c.Param("id"), request.Reason)
	if err != nil {
		return mapError(err)
	}
	return c.JSON(http.StatusOK, order)
}

func mapError(err error) error {
	switch {
	case errors.Is(err, domain.ErrInvalidOrder):
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	case errors.Is(err, domain.ErrNotFound):
		return echo.NewHTTPError(http.StatusNotFound, "order not found")
	case errors.Is(err, domain.ErrConflict):
		return echo.NewHTTPError(http.StatusConflict, "order already exists")
	case errors.Is(err, domain.ErrInvalidTransition):
		return echo.NewHTTPError(http.StatusConflict, err.Error())
	default:
		return err
	}
}

func parsePositiveInt(value string, fallback int) int {
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return fallback
	}
	return parsed
}
