//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type orderResponse struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

func gatewayURL() string {
	if value := os.Getenv("ORDERFLOW_GATEWAY_URL"); value != "" {
		return value
	}
	return "http://localhost:8080"
}

func requireStack(t *testing.T) {
	t.Helper()

	response, err := (&http.Client{Timeout: 5 * time.Second}).Get(gatewayURL() + "/readyz")
	if err != nil {
		t.Skipf("stack is not reachable: %v", err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		t.Skipf("gateway is not ready: status %d", response.StatusCode)
	}
}

func TestConcurrentOrdersReachTerminalState(t *testing.T) {
	requireStack(t)

	const (
		orders      = 100
		concurrency = 10
	)

	client := &http.Client{Timeout: 15 * time.Second}
	var accepted, rejected int64
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, concurrency)
	ids := make([]string, 0, orders)
	var idsMu sync.Mutex

	start := time.Now()
	for i := 0; i < orders; i++ {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(index int) {
			defer wg.Done()
			defer func() { <-semaphore }()

			body, err := json.Marshal(map[string]any{
				"customer_id": fmt.Sprintf("e2e-%d", index),
				"currency":    "EUR",
				"items": []map[string]any{{
					"sku":              "SKU-1",
					"quantity":         1,
					"unit_price_cents": 1999,
				}},
			})
			if err != nil {
				atomic.AddInt64(&rejected, 1)
				return
			}

			request, err := http.NewRequest(http.MethodPost, gatewayURL()+"/v1/orders", bytes.NewReader(body))
			if err != nil {
				atomic.AddInt64(&rejected, 1)
				return
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Idempotency-Key", fmt.Sprintf("e2e-%d-%d", time.Now().UnixNano(), index))

			response, err := client.Do(request)
			if err != nil {
				atomic.AddInt64(&rejected, 1)
				return
			}
			defer func() { _ = response.Body.Close() }()

			if response.StatusCode != http.StatusCreated {
				atomic.AddInt64(&rejected, 1)
				return
			}

			var order orderResponse
			if err := json.NewDecoder(response.Body).Decode(&order); err != nil {
				atomic.AddInt64(&rejected, 1)
				return
			}

			atomic.AddInt64(&accepted, 1)
			idsMu.Lock()
			ids = append(ids, order.OrderID)
			idsMu.Unlock()
		}(i)
	}

	wg.Wait()
	t.Logf("created %d orders in %s (%d rejected)", accepted, time.Since(start).Round(time.Millisecond), rejected)

	if accepted == 0 {
		t.Fatal("no orders were accepted")
	}

	deadline := time.Now().Add(90 * time.Second)
	for _, id := range ids {
		for {
			if time.Now().After(deadline) {
				t.Fatalf("order %s did not reach a terminal state", id)
			}

			response, err := client.Get(gatewayURL() + "/v1/orders/" + id)
			if err != nil {
				t.Fatalf("get order %s: %v", id, err)
			}

			var order orderResponse
			decodeErr := json.NewDecoder(response.Body).Decode(&order)
			response.Body.Close()
			if decodeErr != nil {
				t.Fatalf("decode order %s: %v", id, decodeErr)
			}

			if order.Status == "CONFIRMED" || order.Status == "CANCELLED" {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
	}

	t.Logf("all %d orders reached a terminal state", len(ids))
}
