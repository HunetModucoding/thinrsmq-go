# thinrsmq-go

Thin Redis Streams message queue layer with retry, DLQ, and claim monitoring. Wire-compatible with the Node.js implementation.

## Installation

```bash
go get github.com/your-org/thin-redis-queue/thinrsmq-go
```

## Quick Start

### Producer

```go
package main

import (
    "context"
    "fmt"
    "github.com/redis/go-redis/v9"
    thinrsmq "github.com/your-org/thin-redis-queue/thinrsmq-go/pkg/thinrsmq"
)

func main() {
    client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    cfg := thinrsmq.DefaultConfig()
    cfg.Namespace = "myapp"

    producer := thinrsmq.NewProducer(client, cfg)

    // Publish a single message
    id, err := producer.Publish(context.Background(), "orders", thinrsmq.Message{
        Type:      "order.created",
        Payload:   `{"orderId":"123","amount":99.99}`,
        TraceID:   "trace-abc-123",  // optional
        Producer:  "order-service",   // optional
    })
    if err != nil {
        panic(err)
    }
    fmt.Println("Published message:", id)

    // Publish a batch
    ids, err := producer.PublishBatch(context.Background(), "orders", []thinrsmq.Message{
        {Type: "order.created", Payload: `{"orderId":"124"}`},
        {Type: "order.created", Payload: `{"orderId":"125"}`},
    })
    if err != nil {
        panic(err)
    }
    fmt.Println("Published batch:", ids)
}
```

### Consumer

```go
package main

import (
    "context"
    "fmt"
    "os"
    "os/signal"
    "syscall"
    "github.com/redis/go-redis/v9"
    thinrsmq "github.com/your-org/thin-redis-queue/thinrsmq-go/pkg/thinrsmq"
)

func main() {
    client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    cfg := thinrsmq.DefaultConfig()
    cfg.Namespace = "myapp"

    consumer := thinrsmq.NewConsumer(client, cfg)

    // Subscribe to a topic with a handler
    err := consumer.Subscribe("orders", "order-processor", func(ctx context.Context, msg *thinrsmq.Message) error {
        fmt.Println("Processing order:", msg.Payload)

        // Return error to trigger retry
        // if shouldRetry {
        //     return fmt.Errorf("temporary failure")
        // }

        // Success - message will be acknowledged
        return nil
    })
    if err != nil {
        panic(err)
    }

    // Optional: handle skipped messages
    consumer.OnSkip(func(id, reason string) {
        fmt.Printf("Skipped message: %s, reason: %s\n", id, reason)
    })

    // Graceful shutdown
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan

    fmt.Println("Shutting down...")
    consumer.Stop()
}
```

### Claim Monitor

```go
package main

import (
    "fmt"
    "os"
    "os/signal"
    "syscall"
    "github.com/redis/go-redis/v9"
    thinrsmq "github.com/your-org/thin-redis-queue/thinrsmq-go/pkg/thinrsmq"
)

func main() {
    client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    cfg := thinrsmq.DefaultConfig()
    cfg.Namespace = "myapp"
    cfg.Monitor.Enabled = true
    cfg.Monitor.ScanIntervalMs = 5000
    cfg.Monitor.MinIdleTimeMs = 10000

    monitor := thinrsmq.NewClaimMonitor(client, cfg)
    monitor.Start()

    // Graceful shutdown
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan

    fmt.Println("Shutting down monitor...")
    monitor.Stop()
}
```

### Dead Letter Queue (DLQ)

```go
package main

import (
    "context"
    "fmt"
    "github.com/redis/go-redis/v9"
    thinrsmq "github.com/your-org/thin-redis-queue/thinrsmq-go/pkg/thinrsmq"
)

func main() {
    client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    cfg := thinrsmq.DefaultConfig()
    cfg.Namespace = "myapp"

    dlq := thinrsmq.NewDLQ(client, cfg)
    ctx := context.Background()

    // Peek at DLQ messages
    messages, err := dlq.Peek(ctx, "orders", 10)
    if err != nil {
        panic(err)
    }
    fmt.Println("DLQ messages:", len(messages))

    // Replay a message
    if len(messages) > 0 {
        err = dlq.Replay(ctx, "orders", messages[0].ID)
        if err != nil {
            panic(err)
        }
    }

    // Get DLQ size
    size, err := dlq.Size(ctx, "orders")
    if err != nil {
        panic(err)
    }
    fmt.Println("DLQ size:", size)

    // Purge DLQ
    err = dlq.Purge(ctx, "orders")
    if err != nil {
        panic(err)
    }
}
```

### Admin

```go
package main

import (
    "context"
    "fmt"
    "github.com/redis/go-redis/v9"
    thinrsmq "github.com/your-org/thin-redis-queue/thinrsmq-go/pkg/thinrsmq"
)

func main() {
    client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    cfg := thinrsmq.DefaultConfig()
    cfg.Namespace = "myapp"

    admin := thinrsmq.NewAdmin(client, cfg)
    ctx := context.Background()

    // Get pending stats
    pending, err := admin.PendingStats(ctx, "orders", "order-processor")
    if err != nil {
        panic(err)
    }
    fmt.Printf("Pending messages: %d\n", pending.Count)

    // Get consumer info
    consumers, err := admin.ConsumerInfo(ctx, "orders", "order-processor")
    if err != nil {
        panic(err)
    }
    for _, c := range consumers {
        fmt.Printf("Consumer: %s, Pending: %d\n", c.Name, c.Pending)
    }

    // Get stream info
    stream, err := admin.StreamInfo(ctx, "orders")
    if err != nil {
        panic(err)
    }
    fmt.Printf("Stream length: %d\n", stream.Length)
}
```

## Configuration

### Configuration Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `Namespace` | string | (required) | Namespace for all Redis keys |
| `Redis.Address` | string | `"localhost:6379"` | Redis server address |
| `Redis.Password` | string | `""` | Redis password |
| `Redis.DB` | int | `0` | Redis database number |
| `Redis.PoolSize` | int | `10` | Connection pool size |
| `Redis.ReadTimeoutMs` | int64 | `3000` | Read timeout in milliseconds |
| `Redis.WriteTimeoutMs` | int64 | `3000` | Write timeout in milliseconds |
| `Redis.UseTLS` | bool | `false` | Enable TLS for Redis connection |
| `Streams.DefaultMaxLen` | int64 | `10000` | Default MAXLEN for streams |
| `Consumer.BatchSize` | int | `10` | Number of messages to read per batch |
| `Consumer.BlockMs` | int64 | `5000` | Block duration for XREADGROUP |
| `Consumer.ConsumerName` | string | `"thinrsmq"` | Prefix for consumer name generation |
| `Consumer.ShutdownTimeoutMs` | int64 | `30000` | Graceful shutdown timeout |
| `Retry.MaxAttempts` | int | `3` | Maximum retry attempts before DLQ |
| `Retry.BaseDelayMs` | int64 | `1000` | Base delay for exponential backoff |
| `Retry.MaxDelayMs` | int64 | `60000` | Maximum delay cap |
| `Retry.Jitter` | bool | `true` | Add random jitter to delays |
| `Monitor.Enabled` | bool | `false` | Enable claim monitor |
| `Monitor.ScanIntervalMs` | int64 | `10000` | Interval between PEL scans |
| `Monitor.MinIdleTimeMs` | int64 | `60000` | Minimum idle time to claim |
| `Monitor.ClaimBatchSize` | int | `100` | Max messages to claim per scan |
| `DLQ.MaxReplays` | int | `3` | Max replays before freezing |

### Using Environment Variables

```go
package main

import (
    "crypto/tls"
    "strings"
    "github.com/redis/go-redis/v9"
    thinrsmq "github.com/your-org/thin-redis-queue/thinrsmq-go/pkg/thinrsmq"
)

func main() {
    // Read from environment variables
    cfg := thinrsmq.ConfigFromEnv()
    cfg.Namespace = "myapp"

    // Create Redis client from config
    opts := &redis.Options{
        Addr:     cfg.Redis.Address,
        Password: cfg.Redis.Password,
        DB:       cfg.Redis.DB,
    }

    if cfg.Redis.UseTLS {
        host := strings.Split(cfg.Redis.Address, ":")[0]
        opts.TLSConfig = &tls.Config{ServerName: host}
    }

    client := redis.NewClient(opts)

    // Use the client
    producer := thinrsmq.NewProducer(client, cfg)
    // ...
}
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_HOST` | Redis hostname | `"localhost"` |
| `REDIS_PORT` | Redis port | `"6379"` |
| `REDIS_PASSWORD` | Redis password | `""` |
| `REDIS_USE_TLS` | Enable TLS (`"true"` or `"1"`) | `false` |

## API Reference

For complete API documentation, see [pkg.go.dev](https://pkg.go.dev/github.com/your-org/thin-redis-queue/thinrsmq-go/pkg/thinrsmq).

### Key Types and Functions

#### Producer

```go
type Producer struct { /* ... */ }

func NewProducer(client *redis.Client, cfg Config) *Producer

func (p *Producer) Publish(ctx context.Context, topic string, msg Message) (string, error)
func (p *Producer) PublishBatch(ctx context.Context, topic string, msgs []Message) ([]string, error)
```

#### Consumer

```go
type Consumer struct { /* ... */ }
type Handler func(ctx context.Context, msg *Message) error
type SkipHandler func(id, reason string)

func NewConsumer(client *redis.Client, cfg Config) *Consumer

func (c *Consumer) Subscribe(topic, group string, handler Handler) error
func (c *Consumer) Stop()
func (c *Consumer) OnSkip(handler SkipHandler)
func (c *Consumer) ConsumerName() string
```

#### ClaimMonitor

```go
type ClaimMonitor struct { /* ... */ }

func NewClaimMonitor(client *redis.Client, cfg Config) *ClaimMonitor

func (m *ClaimMonitor) Start()
func (m *ClaimMonitor) Stop()
func (m *ClaimMonitor) ScanOnce(ctx context.Context) error
```

#### DLQ

```go
type DLQ struct { /* ... */ }

func NewDLQ(client *redis.Client, cfg Config) *DLQ

func (d *DLQ) MoveToDLQ(ctx context.Context, topic string, msg Message, metadata FailureMetadata) error
func (d *DLQ) Peek(ctx context.Context, topic string, count int64) ([]DLQMessage, error)
func (d *DLQ) Replay(ctx context.Context, topic, messageID string) error
func (d *DLQ) Purge(ctx context.Context, topic string) error
func (d *DLQ) Size(ctx context.Context, topic string) (int64, error)
```

#### Admin

```go
type Admin struct { /* ... */ }

func NewAdmin(client *redis.Client, cfg Config) *Admin

func (a *Admin) PendingStats(ctx context.Context, topic, group string) (PendingInfo, error)
func (a *Admin) ConsumerInfo(ctx context.Context, topic, group string) ([]ConsumerDetail, error)
func (a *Admin) StreamInfo(ctx context.Context, topic string) (StreamDetail, error)
```

#### RetryStore

```go
type RetryStore struct { /* ... */ }

func NewRetryStore(client *redis.Client, cfg Config) *RetryStore

func (r *RetryStore) InitIfNotExists(ctx context.Context, topic, messageID string) error
func (r *RetryStore) Get(ctx context.Context, topic, messageID string) (*RetryInfo, error)
func (r *RetryStore) Set(ctx context.Context, topic, messageID string, info RetryInfo) error
func (r *RetryStore) Delete(ctx context.Context, topic, messageID string) error
func (r *RetryStore) IncrementAttempt(ctx context.Context, topic, messageID string) (int, error)
```

#### Helper Functions

```go
func DefaultConfig() Config
func ConfigFromEnv() Config
func WithDefaults(cfg Config) Config
func Validate(cfg Config) error

func ComputeDelay(attempt int, cfg BackoffConfig) int64

func StreamKey(namespace, topic string) string
func DLQKey(namespace, topic string) string
func RetryKey(namespace, topic, messageID string) string
```

#### Message Types

```go
type Message struct {
    ID         string
    Version    string
    Type       string
    Payload    string
    TraceID    string
    ProducedAt string
    Producer   string
}

type DLQMessage struct {
    Message
    OriginalID     string
    OriginalStream string
    FailedAt       string
    TotalAttempts  int
    LastError      string
    ConsumerGroup  string
    ReplayCount    int
}
```

## CLI Tools

CLI tools are provided in the `cmd/` directory for development and testing.

### Build CLI Tools

```bash
make build
```

This creates binaries in the `bin/` directory.

### Producer CLI

```bash
./bin/producer -topic orders -type order.created -payload '{"orderId":"123"}'
```

### Consumer CLI

```bash
./bin/consumer -topic orders -group order-processor
```

### Admin CLI

```bash
# Pending stats
./bin/admin -topic orders -group order-processor -command pending

# Consumer info
./bin/admin -topic orders -group order-processor -command consumers

# Stream info
./bin/admin -topic orders -command stream
```

All CLI tools support the standard environment variables (`REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_USE_TLS`).

## Wire Format

Messages use envelope version "1" with the following fields:

| Field | Required | Description |
|-------|----------|-------------|
| `v` | Yes | Envelope version (always "1") |
| `type` | Yes | Message type identifier |
| `payload` | Yes | Message payload (string) |
| `produced_at` | Yes | ISO 8601 timestamp (RFC3339Nano) |
| `trace_id` | No | Trace ID for distributed tracing |
| `producer` | No | Producer identifier |

DLQ messages include additional enrichment fields:

- `original_id`: Original stream entry ID
- `original_stream`: Original stream key
- `failed_at`: Failure timestamp
- `total_attempts`: Total retry attempts
- `last_error`: Error message (truncated to 1000 chars)
- `consumer_group`: Consumer group name
- `replay_count`: Number of times replayed from DLQ

## Cross-Language Interoperability

This Go implementation is wire-compatible with the Node.js implementation. Messages produced by one can be consumed by the other seamlessly.

See [Node.js README](../thinrsmq-node/README.md) for the Node.js implementation.

## License

MIT License. See [LICENSE](./LICENSE) for details.
