// Command replay-dlq moves messages from a dead-letter queue back to the main
// exchange. It is intentionally small so it can be used from a runbook.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	url := flag.String("url", "amqp://orderflow:orderflow@localhost:5672/", "RabbitMQ URL")
	queue := flag.String("queue", "", "dead-letter queue name")
	exchange := flag.String("exchange", "orderflow.events", "target exchange")
	routingKey := flag.String("routing-key", "", "routing key used to republish")
	limit := flag.Int("limit", 100, "maximum messages to replay")
	flag.Parse()

	if *queue == "" || *routingKey == "" {
		log.Fatal("both -queue and -routing-key are required")
	}

	conn, err := amqp.Dial(*url)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("channel: %v", err)
	}
	defer func() { _ = ch.Close() }()

	if err := ch.ExchangeDeclare(*exchange, "topic", true, false, false, false, nil); err != nil {
		log.Fatalf("declare exchange: %v", err)
	}

	deliveries, err := ch.Consume(*queue, "replay-dlq", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("consume %s: %v", *queue, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	replayed := 0
	for replayed < *limit {
		select {
		case <-ctx.Done():
			fmt.Printf("replayed %d messages before timeout\n", replayed)
			return
		case delivery, ok := <-deliveries:
			if !ok {
				fmt.Printf("replayed %d messages\n", replayed)
				return
			}

			headers := amqp.Table{}
			for key, value := range delivery.Headers {
				headers[key] = value
			}
			headers["x-retry-count"] = int32(0)
			headers["x-replayed"] = true

			if err := ch.PublishWithContext(ctx, *exchange, *routingKey, false, false, amqp.Publishing{
				ContentType:   delivery.ContentType,
				DeliveryMode:  amqp.Persistent,
				MessageId:     delivery.MessageId,
				CorrelationId: delivery.CorrelationId,
				Type:          delivery.Type,
				Headers:       headers,
				Body:          delivery.Body,
			}); err != nil {
				_ = delivery.Nack(false, true)
				log.Fatalf("republish: %v", err)
			}

			if err := delivery.Ack(false); err != nil {
				log.Printf("ack failed: %v", err)
			}
			replayed++
		}
	}

	fmt.Printf("replayed %d messages\n", replayed)
}
