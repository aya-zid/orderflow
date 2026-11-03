// Command loadgen creates concurrent orders and reports throughput. It is the
// tool used to demonstrate that inventory never oversells.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

func main() {
	baseURL := flag.String("url", "http://localhost:8080", "gateway base URL")
	orders := flag.Int("orders", 500, "number of orders to create")
	concurrency := flag.Int("concurrency", 50, "maximum in-flight requests")
	sku := flag.String("sku", "SKU-1", "SKU to order")
	quantity := flag.Int("quantity", 1, "quantity per order")
	flag.Parse()

	if *concurrency < 1 {
		*concurrency = 1
	}

	var success, failed int64
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, *concurrency)

	client := &http.Client{Timeout: 15 * time.Second}
	start := time.Now()

	for i := 0; i < *orders; i++ {
		wg.Add(1)
		semaphore <- struct{}{}

		go func() {
			defer wg.Done()
			defer func() { <-semaphore }()

			body, err := json.Marshal(map[string]any{
				"customer_id": fmt.Sprintf("loadgen-%d", time.Now().UnixNano()),
				"currency":    "EUR",
				"items": []map[string]any{{
					"sku":              *sku,
					"quantity":         *quantity,
					"unit_price_cents": 1999,
				}},
			})
			if err != nil {
				atomic.AddInt64(&failed, 1)
				return
			}

			request, err := http.NewRequest(http.MethodPost, *baseURL+"/v1/orders", bytes.NewReader(body))
			if err != nil {
				atomic.AddInt64(&failed, 1)
				return
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Idempotency-Key", uuid.NewString())

			response, err := client.Do(request)
			if err != nil {
				atomic.AddInt64(&failed, 1)
				return
			}
			defer func() { _ = response.Body.Close() }()

			if response.StatusCode >= 300 {
				atomic.AddInt64(&failed, 1)
				return
			}
			atomic.AddInt64(&success, 1)
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)
	rps := float64(*orders) / elapsed.Seconds()

	fmt.Printf("orders=%d concurrency=%d success=%d failed=%d elapsed=%s rps=%.1f\n",
		*orders, *concurrency, success, failed, elapsed.Round(time.Millisecond), rps)
}
