// Package thinrsmq provides a thin message queue layer on top of Redis Streams.
//
// It implements producer, consumer, retry, dead letter queue (DLQ), and claim
// monitoring functionality with wire-compatible message formats across Go and
// Node.js implementations.
//
// # Quick Start
//
// Create a producer:
//
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	cfg := thinrsmq.DefaultConfig()
//	cfg.Namespace = "myapp"
//
//	producer := thinrsmq.NewProducer(client, cfg)
//	id, err := producer.Publish(ctx, "orders", thinrsmq.Message{
//	    Type:    "order.created",
//	    Payload: `{"orderId": "123"}`,
//	})
//
// Create a consumer:
//
//	consumer := thinrsmq.NewConsumer(client, cfg)
//	err := consumer.Subscribe("orders", "order-processor", func(ctx context.Context, msg *thinrsmq.Message) error {
//	    fmt.Println("Received:", msg.Payload)
//	    return nil
//	})
//	defer consumer.Stop()
//
// # Configuration
//
// Use DefaultConfig() for sensible defaults, then override specific fields.
// Use ConfigFromEnv() to read Redis connection details from environment variables.
//
// # Redis Streams Protocol
//
// Messages use envelope version "1" with fields: v, type, payload, produced_at,
// trace_id (optional), producer (optional). The wire format is identical across
// Go and Node.js implementations, enabling cross-language interoperability.
package thinrsmq
