package thinrsmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Handler processes a message. Return nil for success, error for failure.
type Handler func(ctx context.Context, msg *Message) error

// SkipHandler is called when a message is skipped (e.g., unknown version).
type SkipHandler func(id string, reason string)

// Consumer reads messages from a Redis Stream using consumer groups.
type Consumer struct {
	client       *redis.Client
	config       Config
	retryStore   *RetryStore
	consumerName string
	handler      Handler
	skipHandler  SkipHandler
	logger       *slog.Logger

	// Lifecycle
	stopCh  chan struct{}
	doneCh  chan struct{}
	running atomic.Bool
	mu      sync.Mutex

	// Current subscription
	stream string
	group  string
	topic  string
}

// NewConsumer creates a new Consumer with a unique consumer name.
func NewConsumer(client *redis.Client, config Config) *Consumer {
	consumerName := config.Consumer.ConsumerName
	if consumerName == "" {
		consumerName = "thinrsmq"
	}
	consumerName = generateConsumerName(consumerName)

	return &Consumer{
		client:       client,
		config:       config,
		retryStore:   NewRetryStore(client, config),
		consumerName: consumerName,
		logger:       slog.Default(),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}
}

// ConsumerName returns the auto-generated unique consumer name.
func (c *Consumer) ConsumerName() string {
	return c.consumerName
}

// OnSkip registers a handler for skipped messages.
func (c *Consumer) OnSkip(handler SkipHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.skipHandler = handler
}

// Subscribe starts consuming messages from the topic with the given group.
// 1. Ensures group exists (XGROUP CREATE, catches BUSYGROUP)
// 2. Recovery loop: drains pending messages with bounded batches
// 3. Live loop: XREADGROUP BLOCK for new messages
//
// This method starts a goroutine and returns immediately.
// Call Stop() to gracefully shut down.
func (c *Consumer) Subscribe(topic, group string, handler Handler) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running.Load() {
		return errors.New("consumer is already running")
	}

	stream := StreamKey(c.config.Namespace, topic)
	ctx := context.Background()

	// 1. Ensure group exists
	if err := c.ensureGroup(ctx, stream, group); err != nil {
		return fmt.Errorf("failed to ensure group: %w", err)
	}

	c.stream = stream
	c.group = group
	c.topic = topic
	c.handler = handler
	c.running.Store(true)

	// Start read loop in background
	go func() {
		defer close(c.doneCh)

		ctx := context.Background()

		// Phase 1: Recovery loop
		if err := c.recoveryLoop(ctx, stream, group); err != nil {
			c.logger.Error("recovery loop error", "error", err)
		}

		// Phase 2: Live loop
		if err := c.liveLoop(ctx, stream, group); err != nil {
			c.logger.Error("live loop error", "error", err)
		}
	}()

	return nil
}

// Stop gracefully shuts down the consumer.
// Signals the read loop to stop, waits for in-flight messages to finish
// (up to config.Consumer.ShutdownTimeoutMs, default 30s), then force returns.
func (c *Consumer) Stop() error {
	c.mu.Lock()
	if !c.running.Load() {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	close(c.stopCh)

	timeout := time.Duration(c.config.Consumer.ShutdownTimeoutMs) * time.Millisecond
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	select {
	case <-c.doneCh:
		// Clean shutdown
	case <-time.After(timeout):
		// Timeout — force exit
		c.logger.Warn("shutdown timeout exceeded, forcing exit")
	}

	c.running.Store(false)
	return nil
}

// ensureGroup creates the consumer group if it doesn't exist.
// XGROUP CREATE {stream} {group} "0" MKSTREAM
// Catches BUSYGROUP error and treats it as success.
func (c *Consumer) ensureGroup(ctx context.Context, stream, group string) error {
	err := c.client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil {
		// Check for BUSYGROUP error (group already exists)
		if strings.Contains(err.Error(), "BUSYGROUP") {
			return nil // Group already exists — this is fine
		}
		return err
	}
	return nil
}

// recoveryLoop drains pending messages using XAUTOCLAIM with bounded batches.
// Uses XAUTOCLAIM to claim idle messages from any consumer in the group.
// Continues until no more messages are returned.
func (c *Consumer) recoveryLoop(ctx context.Context, stream, group string) error {
	cursor := "0-0"
	batchSize := c.config.Consumer.BatchSize
	if batchSize == 0 {
		batchSize = 10
	}

	// Small delay to ensure messages are considered idle
	time.Sleep(10 * time.Millisecond)

	for {
		select {
		case <-c.stopCh:
			return nil
		default:
		}

		// Use XAUTOCLAIM to claim pending messages from any consumer
		// min-idle-time=1ms (claim messages idle for 1ms or more)
		messages, nextCursor, err := c.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   stream,
			Group:    group,
			Consumer: c.consumerName,
			MinIdle:  time.Millisecond,
			Start:    cursor,
			Count:    int64(batchSize),
		}).Result()

		if err != nil && err != redis.Nil {
			return fmt.Errorf("recovery autoclaim error: %w", err)
		}

		// No more pending messages
		if len(messages) == 0 {
			break
		}

		// Process batch
		for _, xmsg := range messages {
			if err := c.processMessage(ctx, stream, group, c.topic, xmsg.ID, xmsg.Values); err != nil {
				c.logger.Error("recovery process error", "message_id", xmsg.ID, "error", err)
			}
		}

		// Update cursor for next iteration
		cursor = nextCursor

		// If cursor is "0-0", we've completed the full scan
		if nextCursor == "0-0" {
			break
		}
	}

	return nil
}

// liveLoop reads new messages with BLOCK.
// XREADGROUP GROUP {group} {consumer} BLOCK {timeout} COUNT {batch} STREAMS {stream} >
func (c *Consumer) liveLoop(ctx context.Context, stream, group string) error {
	blockTimeout := time.Duration(c.config.Consumer.BlockTimeoutMs) * time.Millisecond
	if blockTimeout == 0 {
		blockTimeout = 5 * time.Second
	}

	batchSize := c.config.Consumer.BatchSize
	if batchSize == 0 {
		batchSize = 10
	}

	for {
		select {
		case <-c.stopCh:
			return nil
		default:
		}

		result, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: c.consumerName,
			Streams:  []string{stream, ">"},
			Count:    batchSize,
			Block:    blockTimeout,
		}).Result()

		if err != nil {
			if err == redis.Nil {
				// Timeout — loop again
				continue
			}
			return fmt.Errorf("live read error: %w", err)
		}

		// Process messages
		for _, xstream := range result {
			for _, xmsg := range xstream.Messages {
				if err := c.processMessage(ctx, stream, group, c.topic, xmsg.ID, xmsg.Values); err != nil {
					c.logger.Error("live process error", "message_id", xmsg.ID, "error", err)
				}
			}
		}
	}
}

// processMessage handles a single message from the stream.
// 1. If fields are nil -> handleNilMessage (trimmed)
// 2. Parse envelope -> if unknown version, skip (XACK + notify skipHandler)
// 3. Call handler
// 4. On success: XACK + delete retry hash
// 5. On failure: initRetryHashIfNotExists via RetryStore
func (c *Consumer) processMessage(
	ctx context.Context,
	stream string,
	group string,
	topic string,
	rawID string,
	rawFields map[string]interface{},
) error {
	// Check for nil/trimmed message
	if rawFields == nil || len(rawFields) == 0 {
		return c.handleNilMessage(ctx, stream, group, topic, rawID)
	}

	// Parse message
	msg, err := MessageFromStreamFields(rawID, rawFields)
	if err != nil {
		if errors.Is(err, ErrUnknownVersion) {
			// Unknown version — XACK and skip
			if err := c.client.XAck(ctx, stream, group, rawID).Err(); err != nil {
				return fmt.Errorf("failed to ACK unknown version message: %w", err)
			}

			c.mu.Lock()
			skipHandler := c.skipHandler
			c.mu.Unlock()

			if skipHandler != nil {
				skipHandler(rawID, "unknown version")
			}

			return nil
		}

		if errors.Is(err, ErrMessageTrimmed) {
			return c.handleNilMessage(ctx, stream, group, topic, rawID)
		}

		return fmt.Errorf("failed to parse message: %w", err)
	}

	// Call handler
	handlerErr := c.handler(ctx, msg)

	if handlerErr == nil {
		// Success — XACK + delete retry hash
		pipe := c.client.Pipeline()
		pipe.XAck(ctx, stream, group, rawID)
		pipe.Del(ctx, RetryKey(c.config.Namespace, topic, rawID))
		_, err := pipe.Exec(ctx)
		return err
	}

	// Failure — init retry hash (does NOT XACK)
	_, err = c.retryStore.InitIfNotExists(ctx, topic, rawID, handlerErr.Error())
	return err
}

// handleNilMessage ACKs a trimmed message and cleans up retry hash.
func (c *Consumer) handleNilMessage(ctx context.Context, stream, group, topic, messageID string) error {
	c.logger.Warn("trimmed message found in PEL, ACKing and skipping", "message_id", messageID)

	pipe := c.client.Pipeline()
	pipe.XAck(ctx, stream, group, messageID)
	pipe.Del(ctx, RetryKey(c.config.Namespace, topic, messageID))

	_, err := pipe.Exec(ctx)
	return err
}

// generateConsumerName creates a unique consumer name.
// Format: {prefix}-{hostname}-{pid}-{short_uuid}
func generateConsumerName(prefix string) string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	pid := os.Getpid()
	shortUUID := uuid.New().String()[:8]

	return fmt.Sprintf("%s-%s-%d-%s", prefix, hostname, pid, shortUUID)
}
