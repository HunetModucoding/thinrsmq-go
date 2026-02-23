package thinrsmq

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// ClaimMonitor scans for idle pending messages and handles retry/DLQ logic.
type ClaimMonitor struct {
	client       *redis.Client
	config       Config
	retryStore   *RetryStore
	handler      Handler
	consumerName string
	logger       *slog.Logger

	// Lifecycle
	stopCh  chan struct{}
	doneCh  chan struct{}
	running atomic.Bool
	mu      sync.Mutex
}

// NewClaimMonitor creates a new ClaimMonitor.
// The handler is called to reprocess claimed messages.
func NewClaimMonitor(
	client *redis.Client,
	config Config,
	handler Handler,
) *ClaimMonitor {
	consumerName := generateMonitorName(config.Monitor.ConsumerName)

	return &ClaimMonitor{
		client:       client,
		config:       config,
		retryStore:   NewRetryStore(client, config),
		handler:      handler,
		consumerName: consumerName,
		logger:       slog.Default(),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}
}

// Start begins the periodic scan loop in a goroutine.
// The monitor scans every config.Monitor.ScanIntervalMs milliseconds.
func (m *ClaimMonitor) Start(topic, group string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running.Load() {
		return nil // already running
	}

	m.stopCh = make(chan struct{})
	m.doneCh = make(chan struct{})
	m.running.Store(true)

	go func() {
		defer close(m.doneCh)
		ticker := time.NewTicker(time.Duration(m.config.Monitor.ScanIntervalMs) * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-m.stopCh:
				return
			case <-ticker.C:
				ctx := context.Background()
				if _, err := m.ScanOnce(ctx, topic, group); err != nil {
					m.logger.Error("scan failed", "error", err)
				}
			}
		}
	}()

	return nil
}

// Stop gracefully stops the monitor.
func (m *ClaimMonitor) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running.Load() {
		return nil
	}

	close(m.stopCh)
	<-m.doneCh
	m.running.Store(false)
	return nil
}

// ScanOnce performs a single scan cycle. Exposed for testing.
// Returns the number of messages processed (claimed + DLQ'd + cleaned).
func (m *ClaimMonitor) ScanOnce(ctx context.Context, topic, group string) (int, error) {
	stream := StreamKey(m.config.Namespace, topic)
	count := 0

	// Step 1: XPENDING IDLE to find idle messages
	pending, err := m.scanPending(ctx, stream, group)
	if err != nil {
		return 0, fmt.Errorf("scan pending failed: %w", err)
	}

	eligibleIDs := []string{}

	// Step 2: For each pending entry, check retry state
	for _, entry := range pending {
		msgID := entry.ID

		// Read retry metadata
		retryInfo, err := m.retryStore.Get(ctx, topic, msgID)
		if err != nil {
			m.logger.Error("failed to get retry info", "message_id", msgID, "error", err)
			continue
		}

		if retryInfo == nil {
			// Consumer crashed before writing retry hash (spec section 7.2)
			// Initialize retry hash -- monitor takes ownership
			delay := ComputeDelay(1, BackoffConfig{
				BaseDelayMs: m.config.Retry.BaseDelayMs,
				MaxDelayMs:  m.config.Retry.MaxDelayMs,
				Jitter:      m.config.Retry.Jitter,
			})
			now := time.Now().UTC().UnixMilli()
			if err := m.retryStore.Set(ctx, topic, msgID, RetryInfo{
				Attempt:       1,
				MaxAttempts:   m.config.Retry.MaxAttempts,
				NextRetryAt:   now + delay,
				LastError:     "consumer did not ACK (crash or timeout)",
				FirstFailedAt: now,
			}); err != nil {
				m.logger.Error("failed to init retry hash", "message_id", msgID, "error", err)
			}
			continue // skip this cycle, let backoff elapse
		}

		// Check if retries exhausted -> move to DLQ
		if retryInfo.Attempt >= retryInfo.MaxAttempts {
			// Fetch original message for DLQ enrichment
			entries, err := m.client.XRangeN(ctx, stream, msgID, msgID, 1).Result()
			if err != nil {
				m.logger.Error("failed to fetch message for DLQ", "message_id", msgID, "error", err)
				continue
			}

			if len(entries) == 0 || entries[0].Values == nil || len(entries[0].Values) == 0 {
				// Trimmed message -- nothing to DLQ, just clean up
				if err := m.handleTrimmedMessage(ctx, stream, group, topic, msgID); err != nil {
					m.logger.Error("failed to handle trimmed message", "message_id", msgID, "error", err)
				} else {
					count++
				}
				continue
			}

			// Move to DLQ via pipeline
			if err := m.moveToDLQ(ctx, stream, group, topic, msgID, retryInfo); err != nil {
				m.logger.Error("failed to move to DLQ", "message_id", msgID, "error", err)
			} else {
				count++
			}
			continue
		}

		// Check if backoff has elapsed
		now := time.Now().UTC().UnixMilli()
		if now < retryInfo.NextRetryAt {
			continue // backoff not elapsed, skip
		}

		// Eligible for claiming
		eligibleIDs = append(eligibleIDs, msgID)
	}

	// Step 3: Batch-claim all eligible messages
	if len(eligibleIDs) > 0 {
		claimed, err := m.client.XClaim(ctx, &redis.XClaimArgs{
			Stream:   stream,
			Group:    group,
			Consumer: m.consumerName,
			MinIdle:  time.Duration(m.config.Monitor.MinIdleMs) * time.Millisecond,
			Messages: eligibleIDs,
		}).Result()

		if err != nil {
			return count, fmt.Errorf("xclaim failed: %w", err)
		}

		// Step 4: Process each claimed message
		for _, message := range claimed {
			if message.Values == nil || len(message.Values) == 0 {
				// Trimmed after XPENDING but before XCLAIM
				if err := m.handleTrimmedMessage(ctx, stream, group, topic, message.ID); err != nil {
					m.logger.Error("failed to handle trimmed message", "message_id", message.ID, "error", err)
				} else {
					count++
				}
				continue
			}

			// Parse envelope
			msg, err := MessageFromStreamFields(message.ID, message.Values)
			if err != nil {
				// Check if it's an unknown version error
				if err == ErrUnknownVersion {
					// Unknown version -- XACK and skip
					if err := m.client.XAck(ctx, stream, group, message.ID).Err(); err != nil {
						m.logger.Error("failed to ACK unknown version message", "message_id", message.ID, "error", err)
					}
					count++
					continue
				} else if err == ErrMessageTrimmed {
					if err := m.handleTrimmedMessage(ctx, stream, group, topic, message.ID); err != nil {
						m.logger.Error("failed to handle trimmed message", "message_id", message.ID, "error", err)
					} else {
						count++
					}
					continue
				}
				m.logger.Error("failed to parse message", "message_id", message.ID, "error", err)
				continue
			}

			// Reprocess with handler
			handlerErr := m.handler(ctx, msg)
			if handlerErr == nil {
				// Success
				if err := m.handleSuccessfulProcess(ctx, stream, group, topic, message.ID); err != nil {
					m.logger.Error("failed to handle successful process", "message_id", message.ID, "error", err)
				} else {
					count++
				}
			} else {
				// Failure -- increment attempt
				retryInfo, err := m.retryStore.Get(ctx, topic, message.ID)
				if err != nil {
					m.logger.Error("failed to get retry info after failure", "message_id", message.ID, "error", err)
					continue
				}
				if retryInfo != nil {
					if err := m.handleFailedProcess(ctx, topic, message.ID, retryInfo.Attempt, handlerErr.Error()); err != nil {
						m.logger.Error("failed to handle failed process", "message_id", message.ID, "error", err)
					}
				}
				count++
			}
		}
	}

	return count, nil
}

// scanPending calls XPENDING IDLE and returns pending entries.
func (m *ClaimMonitor) scanPending(
	ctx context.Context,
	stream, group string,
) ([]redis.XPendingExt, error) {
	return m.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Idle:   time.Duration(m.config.Monitor.MinIdleMs) * time.Millisecond,
		Start:  "-",
		End:    "+",
		Count:  int64(m.config.Monitor.BatchSize),
	}).Result()
}

// moveToDLQ moves a message to the DLQ stream via pipeline.
// PIPELINE: XADD dlq + XACK main + DEL retry hash
func (m *ClaimMonitor) moveToDLQ(
	ctx context.Context,
	stream, group, topic string,
	msgID string,
	retryInfo *RetryInfo,
) error {
	// Fetch original message
	entries, err := m.client.XRangeN(ctx, stream, msgID, msgID, 1).Result()
	if err != nil {
		return fmt.Errorf("failed to fetch original message: %w", err)
	}
	if len(entries) == 0 {
		return m.handleTrimmedMessage(ctx, stream, group, topic, msgID)
	}

	original, err := MessageFromStreamFields(msgID, entries[0].Values)
	if err != nil {
		return m.handleTrimmedMessage(ctx, stream, group, topic, msgID)
	}

	// Check replay count from original message (set by DLQ replay)
	replayCount := 0
	if rc, ok := entries[0].Values["_dlq_replay_count"]; ok {
		if rcStr, ok := rc.(string); ok {
			replayCount, _ = strconv.Atoi(rcStr)
		}
	}

	dlqMsg := NewDLQMessage(original, stream, retryInfo.LastError, retryInfo.Attempt, group)
	dlqMsg.ReplayCount = replayCount
	dlqFields := dlqMsg.ToStreamFields()

	dlqStream := DLQKey(m.config.Namespace, topic)
	retryHashKey := RetryKey(m.config.Namespace, topic, msgID)

	pipe := m.client.Pipeline()
	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: dlqStream,
		MaxLen: m.config.Streams.DLQMaxLen,
		Approx: true,
		ID:     "*",
		Values: dlqFields,
	})
	pipe.XAck(ctx, stream, group, msgID)
	pipe.Del(ctx, retryHashKey)
	_, err = pipe.Exec(ctx)
	return err
}

// handleTrimmedMessage ACKs a trimmed message and cleans up.
// PIPELINE: XACK + DEL retry hash
func (m *ClaimMonitor) handleTrimmedMessage(
	ctx context.Context,
	stream, group, topic string,
	msgID string,
) error {
	retryHashKey := RetryKey(m.config.Namespace, topic, msgID)

	pipe := m.client.Pipeline()
	pipe.XAck(ctx, stream, group, msgID)
	pipe.Del(ctx, retryHashKey)
	_, err := pipe.Exec(ctx)

	if err == nil {
		m.logger.Warn("trimmed message cleaned up from PEL", "message_id", msgID)
	}

	return err
}

// handleSuccessfulProcess ACKs and cleans up after successful reprocessing.
// PIPELINE: XACK + DEL retry hash
func (m *ClaimMonitor) handleSuccessfulProcess(
	ctx context.Context,
	stream, group, topic string,
	msgID string,
) error {
	retryHashKey := RetryKey(m.config.Namespace, topic, msgID)

	pipe := m.client.Pipeline()
	pipe.XAck(ctx, stream, group, msgID)
	pipe.Del(ctx, retryHashKey)
	_, err := pipe.Exec(ctx)
	return err
}

// handleFailedProcess increments attempt and updates backoff.
func (m *ClaimMonitor) handleFailedProcess(
	ctx context.Context,
	topic string,
	msgID string,
	currentAttempt int,
	errorMsg string,
) error {
	nextAttempt := currentAttempt + 1
	delay := ComputeDelay(nextAttempt, BackoffConfig{
		BaseDelayMs: m.config.Retry.BaseDelayMs,
		MaxDelayMs:  m.config.Retry.MaxDelayMs,
		Jitter:      m.config.Retry.Jitter,
	})
	now := time.Now().UTC().UnixMilli()
	return m.retryStore.IncrementAttempt(ctx, topic, msgID, nextAttempt, now+delay, errorMsg)
}

// generateMonitorName creates a unique monitor consumer name.
// Format: {prefix}-{hostname}-{pid}-{uuid_short}
func generateMonitorName(prefix string) string {
	if prefix == "" {
		prefix = "monitor"
	}
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}
	pid := os.Getpid()
	uuidShort := uuid.New().String()[:8]
	return fmt.Sprintf("%s-%s-%d-%s", prefix, hostname, pid, uuidShort)
}
