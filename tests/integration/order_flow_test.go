//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

type orderResponse struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
	History []struct {
		From   string `json:"from"`
		To     string `json:"to"`
		Reason string `json:"reason"`
	} `json:"history"`
}

func gatewayURL() string {
	if value := os.Getenv("ORDERFLOW_GATEWAY_URL"); value != "" {
		return value
	}
	return "http://localhost:8080"
}

func httpClient() *http.Client {
	return &http.Client{Timeout: 10 * time.Second}
}

func requireStack(t *testing.T) {
	t.Helper()

	response, err := httpClient().Get(gatewayURL() + "/readyz")
	if err != nil {
		t.Skipf("stack is not reachable: %v", err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		t.Skipf("gateway is not ready: status %d", response.StatusCode)
	}
}

func createOrder(t *testing.T, idempotencyKey, sku string) (orderResponse, int) {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"customer_id": "integration-" + time.Now().Format("150405.000000"),
		"currency":    "EUR",
		"items": []map[string]any{{
			"sku":              sku,
			"quantity":         1,
			"unit_price_cents": 1999,
		}},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	request, err := http.NewRequest(http.MethodPost, gatewayURL()+"/v1/orders", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", idempotencyKey)

	response, err := httpClient().Do(request)
	if err != nil {
		t.Fatalf("create order: %v", err)
	}
	defer func() { _ = response.Body.Close() }()

	var order orderResponse
	if response.StatusCode == http.StatusCreated {
		if err := json.NewDecoder(response.Body).Decode(&order); err != nil {
			t.Fatalf("decode order: %v", err)
		}
	}
	return order, response.StatusCode
}

func getOrder(t *testing.T, orderID string) (orderResponse, int) {
	t.Helper()

	response, err := httpClient().Get(gatewayURL() + "/v1/orders/" + orderID)
	if err != nil {
		t.Fatalf("get order: %v", err)
	}
	defer func() { _ = response.Body.Close() }()

	var order orderResponse
	if response.StatusCode == http.StatusOK {
		if err := json.NewDecoder(response.Body).Decode(&order); err != nil {
			t.Fatalf("decode order: %v", err)
		}
	}
	return order, response.StatusCode
}

func waitForTerminal(t *testing.T, orderID string, timeout time.Duration) orderResponse {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		order, status := getOrder(t, orderID)
		if status == http.StatusOK && (order.Status == "CONFIRMED" || order.Status == "CANCELLED") {
			return order
		}
		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("order %s did not reach a terminal state within %s", orderID, timeout)
	return orderResponse{}
}

func TestOrderLifecycleAndIdempotency(t *testing.T) {
	requireStack(t)

	key := fmt.Sprintf("integration-%d", time.Now().UnixNano())
	first, status := createOrder(t, key, "SKU-1")
	if status != http.StatusCreated {
		t.Fatalf("expected 201, got %d", status)
	}
	if first.OrderID == "" {
		t.Fatal("expected an order_id")
	}

	replay, status := createOrder(t, key, "SKU-1")
	if status != http.StatusCreated {
		t.Fatalf("expected 201 on replay, got %d", status)
	}
	if replay.OrderID != first.OrderID {
		t.Fatalf("idempotency failed: %s != %s", replay.OrderID, first.OrderID)
	}

	final := waitForTerminal(t, first.OrderID, 60*time.Second)
	if final.Status != "CONFIRMED" && final.Status != "CANCELLED" {
		t.Fatalf("unexpected terminal status %q", final.Status)
	}
	if len(final.History) < 2 {
		t.Fatalf("expected a status timeline, got %d entries", len(final.History))
	}
}

func TestCreateRequiresIdempotencyKey(t *testing.T) {
	requireStack(t)

	body, err := json.Marshal(map[string]any{
		"customer_id": "integration-no-key",
		"currency":    "EUR",
		"items": []map[string]any{{
			"sku":              "SKU-1",
			"quantity":         1,
			"unit_price_cents": 1999,
		}},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	request, err := http.NewRequest(http.MethodPost, gatewayURL()+"/v1/orders", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := httpClient().Do(request)
	if err != nil {
		t.Fatalf("create order: %v", err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 without Idempotency-Key, got %d", response.StatusCode)
	}
}

func TestCancelUnknownOrder(t *testing.T) {
	requireStack(t)

	request, err := http.NewRequest(
		http.MethodPost,
		gatewayURL()+"/v1/orders/00000000-0000-0000-0000-000000000000/cancel",
		nil,
	)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}

	response, err := httpClient().Do(request)
	if err != nil {
		t.Fatalf("cancel order: %v", err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown order, got %d", response.StatusCode)
	}
}
