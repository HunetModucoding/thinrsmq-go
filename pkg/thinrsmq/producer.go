package thinrsmq

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

// BackPressureWarning is emitted when stream length exceeds 80% of max.
type BackPressureWarning struct {
	Stream     string
	CurrentLen int64
	MaxLen     int64
}

// Producer publishes messages to a Redis Stream.
type Producer struct {
	client   *redis.Client
	config   Config
	logger   *slog.Logger
	warnings []BackPressureWarning
}

// NewProducer creates a new Producer.
func NewProducer(client *redis.Client, config Config) *Producer {
	return &Producer{
		client:   client,
		config:   config,
		logger:   slog.Default(),
		warnings: make([]BackPressureWarning, 0),
	}
}

// Publish sends a single message to the specified topic.
// Returns the stream entry ID (e.g., "1678886400123-0").
//
// Redis command: XADD {namespace}:{topic} MAXLEN ~ {defaultMaxLen} *
//
//	v {version} type {type} payload {payload} produced_at {iso} [trace_id {id}] [producer {name}]
func (p *Producer) Publish(ctx context.Context, topic string, msg Message) (string, error) {
	// Check back-pressure before publishing
	if err := p.checkBackPressure(ctx, topic); err != nil {
		return "", err
	}

	streamKey := StreamKey(p.config.Namespace, topic)
	fields := msg.ToStreamFields()

	id, err := p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: p.config.Streams.DefaultMaxLen,
		Approx: true, // CRITICAL: use ~ for performance
		ID:     "*",
		Values: fields,
	}).Result()

	if err != nil {
		return "", fmt.Errorf("xadd failed: %w", err)
	}

	return id, nil
}

// PublishBatch sends multiple messages in a Redis pipeline.
// Returns a slice of stream entry IDs in the same order as the input.
func (p *Producer) PublishBatch(ctx context.Context, topic string, msgs []Message) ([]string, error) {
	if len(msgs) == 0 {
		return []string{}, nil
	}

	// Check back-pressure once before the batch
	if err := p.checkBackPressure(ctx, topic); err != nil {
		return nil, err
	}

	streamKey := StreamKey(p.config.Namespace, topic)

	pipe := p.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(msgs))

	for i, msg := range msgs {
		fields := msg.ToStreamFields()
		cmds[i] = pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			MaxLen: p.config.Streams.DefaultMaxLen,
			Approx: true,
			ID:     "*",
			Values: fields,
		})
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("pipeline failed: %w", err)
	}

	ids := make([]string, len(cmds))
	for i, cmd := range cmds {
		id, err := cmd.Result()
		if err != nil {
			return nil, fmt.Errorf("get result for message %d: %w", i, err)
		}
		ids[i] = id
	}

	return ids, nil
}

// Warnings returns accumulated back-pressure warnings (primarily for testing).
func (p *Producer) Warnings() []BackPressureWarning {
	return p.warnings
}

// checkBackPressure checks XLEN and logs a warning if > 80% of MaxStreamLen.
func (p *Producer) checkBackPressure(ctx context.Context, topic string) error {
	streamKey := StreamKey(p.config.Namespace, topic)

	length, err := p.client.XLen(ctx, streamKey).Result()
	if err != nil {
		// If stream doesn't exist yet, XLEN returns 0 (no error in go-redis)
		// Just continue - this is the first message
		return nil
	}

	threshold := int64(float64(p.config.Streams.DefaultMaxLen) * 0.8)
	if length > threshold {
		warning := BackPressureWarning{
			Stream:     streamKey,
			CurrentLen: length,
			MaxLen:     p.config.Streams.DefaultMaxLen,
		}
		p.warnings = append(p.warnings, warning)
		p.logger.Warn(
			"stream approaching trim threshold",
			"stream", streamKey,
			"current_len", length,
			"max_len", p.config.Streams.DefaultMaxLen,
			"threshold", threshold,
		)
	}

	return nil
}
