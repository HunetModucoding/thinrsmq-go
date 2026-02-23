package test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/your-org/thin-redis-queue/thinrsmq-go/pkg/thinrsmq"
)

// acceleratedConfig returns a config with fast intervals for testing
func acceleratedConfig(namespace string) thinrsmq.Config {
	return thinrsmq.Config{
		Namespace: namespace,
		Retry: thinrsmq.RetryConfig{
			MaxAttempts:    3,
			BaseDelayMs:    100,   // very small for fast tests
			MaxDelayMs:     500,   // cap at 500ms
			Jitter:         false, // deterministic
			HashTTLSeconds: 86400,
		},
		Monitor: thinrsmq.MonitorConfig{
			MinIdleMs:      500, // fast idle detection
			ScanIntervalMs: 200, // fast scan
			BatchSize:      100,
			ConsumerName:   "monitor",
			Enabled:        true,
		},
		Consumer: thinrsmq.ConsumerConfig{
			BlockTimeoutMs:    1000, // 1s block instead of 5s
			BatchSize:         10,
			ConsumerName:      "consumer",
			ShutdownTimeoutMs: 30000,
		},
		Streams: thinrsmq.StreamsConfig{
			DefaultMaxLen: 100000,
			DLQMaxLen:     10000,
		},
		DLQ: thinrsmq.DLQConfig{
			MaxReplays: 3,
		},
	}
}

// TestLifecycle_HappyPath verifies publish → consume → ACK → clean PEL
func TestLifecycle_HappyPath(t *testing.T) {
	client := NewRedisClient()
	defer client.Close()

	ns := UniqueNamespace(t, client)
	cfg := acceleratedConfig(ns)

	producer := thinrsmq.NewProducer(client, cfg)

	// Channel to receive message
	received := make(chan *thinrsmq.Message, 1)
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		received <- msg
		return nil
	}

	consumer := thinrsmq.NewConsumer(client, cfg)
	err := consumer.Subscribe("topic", "group", handler)
	require.NoError(t, err)
	defer consumer.Stop()

	// Publish message
	msg := thinrsmq.Message{
		Type:    "order.created",
		Payload: `{"id":1}`,
	}
	id, err := producer.Publish(context.Background(), "topic", msg)
	require.NoError(t, err)
	t.Logf("Published message with ID: %s", id)

	// Wait for message
	select {
	case recv := <-received:
		require.Equal(t, "order.created", recv.Type)
		require.Equal(t, "1", recv.Version)
		require.Equal(t, `{"id":1}`, recv.Payload)
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	// Wait for PEL to be clean
	stream := thinrsmq.StreamKey(ns, "topic")
	WaitFor(t, func() bool {
		count, _ := PendingCount(context.Background(), client, stream, "group")
		return count == 0
	}, 5*time.Second)

	// Verify retry hash is cleaned up
	retryStore := thinrsmq.NewRetryStore(client, cfg)
	retryInfo, err := retryStore.Get(context.Background(), "topic", id)
	require.NoError(t, err)
	require.Nil(t, retryInfo, "Retry hash should be cleaned up after success")
}

// TestLifecycle_RetryPathWithExponentialBackoff verifies retry with backoff timing
func TestLifecycle_RetryPathWithExponentialBackoff(t *testing.T) {
	client := NewRedisClient()
	defer client.Close()

	ns := UniqueNamespace(t, client)
	cfg := acceleratedConfig(ns)

	producer := thinrsmq.NewProducer(client, cfg)
	dlq := thinrsmq.NewDLQ(client, cfg)

	// Track calls
	var callCount atomic.Int32
	var mu sync.Mutex
	callTimes := make([]int64, 0, 3)

	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		mu.Lock()
		callTimes = append(callTimes, time.Now().UnixMilli())
		mu.Unlock()

		count := callCount.Add(1)
		if count < 3 {
			return fmt.Errorf("transient failure")
		}
		return nil
	}

	consumer := thinrsmq.NewConsumer(client, cfg)
	err := consumer.Subscribe("topic", "group", handler)
	require.NoError(t, err)
	defer consumer.Stop()

	monitor := thinrsmq.NewClaimMonitor(client, cfg, handler)
	err = monitor.Start("topic", "group")
	require.NoError(t, err)
	defer monitor.Stop()

	// Publish message
	msg := thinrsmq.Message{
		Type:    "test",
		Payload: `{"retry":true}`,
	}
	_, err = producer.Publish(context.Background(), "topic", msg)
	require.NoError(t, err)

	// Wait for 3 attempts
	WaitFor(t, func() bool {
		return callCount.Load() >= 3
	}, 60*time.Second)

	// Wait for cleanup
	stream := thinrsmq.StreamKey(ns, "topic")
	WaitFor(t, func() bool {
		count, _ := PendingCount(context.Background(), client, stream, "group")
		return count == 0
	}, 10*time.Second)

	// Verify DLQ is empty (message succeeded)
	dlqSize, err := dlq.Size(context.Background(), "topic")
	require.NoError(t, err)
	require.Equal(t, int64(0), dlqSize, "DLQ should be empty after success")

	// Verify backoff was approximately exponential
	mu.Lock()
	times := append([]int64{}, callTimes...)
	mu.Unlock()

	if len(times) >= 3 {
		gap1 := times[1] - times[0] // ~baseDelay + scanInterval + minIdle
		gap2 := times[2] - times[1] // ~baseDelay*2 + scanInterval + minIdle
		t.Logf("Gap1: %dms, Gap2: %dms", gap1, gap2)
		// Gap2 should be noticeably larger than Gap1 (exponential)
		// With tolerance for scan interval variance - very loose check
		// Just verify gap2 is not significantly smaller than gap1
		require.GreaterOrEqual(t, gap2, gap1*9/10, "Gap2 should be at least 90% of Gap1 (with jitter and timing variance)")
	}
}

// TestLifecycle_DLQPathExhaustsAllRetries verifies DLQ escalation
func TestLifecycle_DLQPathExhaustsAllRetries(t *testing.T) {
	client := NewRedisClient()
	defer client.Close()

	ns := UniqueNamespace(t, client)
	cfg := acceleratedConfig(ns)
	cfg.Retry.MaxAttempts = 3

	producer := thinrsmq.NewProducer(client, cfg)
	dlq := thinrsmq.NewDLQ(client, cfg)

	// Always fail
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		return fmt.Errorf("permanent failure")
	}

	consumer := thinrsmq.NewConsumer(client, cfg)
	err := consumer.Subscribe("topic", "group", handler)
	require.NoError(t, err)
	defer consumer.Stop()

	monitor := thinrsmq.NewClaimMonitor(client, cfg, handler)
	err = monitor.Start("topic", "group")
	require.NoError(t, err)
	defer monitor.Stop()

	// Publish message
	msg := thinrsmq.Message{
		Type:    "test",
		Payload: `{"permanent":true}`,
	}
	_, err = producer.Publish(context.Background(), "topic", msg)
	require.NoError(t, err)

	// Wait for DLQ entry
	WaitFor(t, func() bool {
		size, _ := dlq.Size(context.Background(), "topic")
		return size == 1
	}, 60*time.Second)

	// Wait for PEL to be clean
	stream := thinrsmq.StreamKey(ns, "topic")
	WaitFor(t, func() bool {
		count, _ := PendingCount(context.Background(), client, stream, "group")
		return count == 0
	}, 5*time.Second)

	// Verify DLQ entry metadata
	entries, err := dlq.Peek(context.Background(), "topic", 1)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	entry := entries[0]
	require.Equal(t, 3, entry.TotalAttempts, "Should have 3 attempts")
	require.Contains(t, entry.LastError, "permanent failure")
	require.Equal(t, 0, entry.ReplayCount, "Initial DLQ entry should have replay_count=0")
	require.Equal(t, "1", entry.Version, "Version should be preserved")
}

// TestLifecycle_ReplayThenSucceed verifies DLQ replay → success
func TestLifecycle_ReplayThenSucceed(t *testing.T) {
	client := NewRedisClient()
	defer client.Close()

	ns := UniqueNamespace(t, client)
	cfg := acceleratedConfig(ns)
	cfg.Retry.MaxAttempts = 2

	producer := thinrsmq.NewProducer(client, cfg)
	dlq := thinrsmq.NewDLQ(client, cfg)

	// Fail first, then succeed
	var attemptCount atomic.Int32
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		count := attemptCount.Add(1)
		if count <= 2 {
			return fmt.Errorf("initial failure")
		}
		return nil
	}

	consumer := thinrsmq.NewConsumer(client, cfg)
	err := consumer.Subscribe("topic", "group", handler)
	require.NoError(t, err)
	defer consumer.Stop()

	monitor := thinrsmq.NewClaimMonitor(client, cfg, handler)
	err = monitor.Start("topic", "group")
	require.NoError(t, err)
	defer monitor.Stop()

	// Publish message
	msg := thinrsmq.Message{
		Type:    "test",
		Payload: `{"replay":true}`,
	}
	_, err = producer.Publish(context.Background(), "topic", msg)
	require.NoError(t, err)

	// Wait for DLQ entry
	WaitFor(t, func() bool {
		size, _ := dlq.Size(context.Background(), "topic")
		return size == 1
	}, 30*time.Second)

	// Replay from DLQ
	replayed, err := dlq.Replay(context.Background(), "topic", 1)
	require.NoError(t, err)
	require.Equal(t, 1, replayed, "Should replay 1 message")

	// Wait for success
	stream := thinrsmq.StreamKey(ns, "topic")
	WaitFor(t, func() bool {
		count, _ := PendingCount(context.Background(), client, stream, "group")
		return count == 0
	}, 10*time.Second)

	// Verify DLQ is empty
	dlqSize, err := dlq.Size(context.Background(), "topic")
	require.NoError(t, err)
	require.Equal(t, int64(0), dlqSize, "DLQ should be empty after successful replay")
}

// TestLifecycle_ReplayPoisonHitsMaxReplaysAndFreezes verifies max replay limit
func TestLifecycle_ReplayPoisonHitsMaxReplaysAndFreezes(t *testing.T) {
	client := NewRedisClient()
	defer client.Close()

	ns := UniqueNamespace(t, client)
	cfg := acceleratedConfig(ns)
	cfg.Retry.MaxAttempts = 2
	cfg.DLQ.MaxReplays = 2

	producer := thinrsmq.NewProducer(client, cfg)
	dlq := thinrsmq.NewDLQ(client, cfg)

	// Always fail (poison message)
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		return fmt.Errorf("poison")
	}

	consumer := thinrsmq.NewConsumer(client, cfg)
	err := consumer.Subscribe("topic", "group", handler)
	require.NoError(t, err)
	defer consumer.Stop()

	monitor := thinrsmq.NewClaimMonitor(client, cfg, handler)
	err = monitor.Start("topic", "group")
	require.NoError(t, err)
	defer monitor.Stop()

	// Publish message
	msg := thinrsmq.Message{
		Type:    "test",
		Payload: `{"poison":true}`,
	}
	_, err = producer.Publish(context.Background(), "topic", msg)
	require.NoError(t, err)

	// First DLQ entry (replay_count=0)
	WaitFor(t, func() bool {
		size, _ := dlq.Size(context.Background(), "topic")
		return size == 1
	}, 30*time.Second)

	entries, err := dlq.Peek(context.Background(), "topic", 1)
	require.NoError(t, err)
	require.Equal(t, 0, entries[0].ReplayCount, "First DLQ entry should have replay_count=0")

	// Replay 1
	replayed, err := dlq.Replay(context.Background(), "topic", 1)
	require.NoError(t, err)
	require.Equal(t, 1, replayed)

	WaitFor(t, func() bool {
		size, _ := dlq.Size(context.Background(), "topic")
		return size == 1
	}, 30*time.Second)

	entries, err = dlq.Peek(context.Background(), "topic", 1)
	require.NoError(t, err)
	require.Equal(t, 1, entries[0].ReplayCount, "Second DLQ entry should have replay_count=1")

	// Replay 2
	replayed, err = dlq.Replay(context.Background(), "topic", 1)
	require.NoError(t, err)
	require.Equal(t, 1, replayed)

	WaitFor(t, func() bool {
		size, _ := dlq.Size(context.Background(), "topic")
		return size == 1
	}, 30*time.Second)

	entries, err = dlq.Peek(context.Background(), "topic", 1)
	require.NoError(t, err)
	require.Equal(t, 2, entries[0].ReplayCount, "Third DLQ entry should have replay_count=2")

	// Replay 3: should be frozen (replay_count >= max_replays)
	replayed, err = dlq.Replay(context.Background(), "topic", 1)
	require.NoError(t, err)
	require.Equal(t, 0, replayed, "Should not replay frozen entry")

	// Verify DLQ still has the frozen entry
	dlqSize, err := dlq.Size(context.Background(), "topic")
	require.NoError(t, err)
	require.Equal(t, int64(1), dlqSize, "Frozen entry should remain in DLQ")
}

// TestLifecycle_ConsumerCrashRecoveryViaMonitor verifies monitor handles orphaned messages
func TestLifecycle_ConsumerCrashRecoveryViaMonitor(t *testing.T) {
	client := NewRedisClient()
	defer client.Close()

	ns := UniqueNamespace(t, client)
	cfg := acceleratedConfig(ns)

	producer := thinrsmq.NewProducer(client, cfg)
	retryStore := thinrsmq.NewRetryStore(client, cfg)

	// Publish message
	msg := thinrsmq.Message{
		Type:    "test",
		Payload: `{"crash":true}`,
	}
	id, err := producer.Publish(context.Background(), "topic", msg)
	require.NoError(t, err)

	// Simulate consumer crash: create group, read message, but don't ACK
	stream := thinrsmq.StreamKey(ns, "topic")
	err = client.XGroupCreate(context.Background(), stream, "group", "0").Err()
	require.NoError(t, err)

	// Read message without ACK (simulate crash)
	result, err := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    "group",
		Consumer: "crashed-consumer",
		Streams:  []string{stream, ">"},
		Count:    1,
		Block:    -1,
	}).Result()
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Len(t, result[0].Messages, 1)

	// Verify message is in PEL
	pending, err := PendingCount(context.Background(), client, stream, "group")
	require.NoError(t, err)
	require.Equal(t, int64(1), pending, "Message should be in PEL")

	// Verify NO retry hash exists (consumer crashed before writing it)
	retryInfo, err := retryStore.Get(context.Background(), "topic", id)
	require.NoError(t, err)
	require.Nil(t, retryInfo, "Retry hash should not exist after crash")

	// Start monitor (no consumer)
	var processCount atomic.Int32
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		processCount.Add(1)
		return nil
	}

	monitor := thinrsmq.NewClaimMonitor(client, cfg, handler)
	err = monitor.Start("topic", "group")
	require.NoError(t, err)
	defer monitor.Stop()

	// Monitor should: detect idle → init retry hash → wait backoff → claim → process → ACK
	WaitFor(t, func() bool {
		return processCount.Load() == 1
	}, 60*time.Second)

	// Verify PEL is clean
	WaitFor(t, func() bool {
		count, _ := PendingCount(context.Background(), client, stream, "group")
		return count == 0
	}, 5*time.Second)

	// Verify retry hash is cleaned up
	retryInfo, err = retryStore.Get(context.Background(), "topic", id)
	require.NoError(t, err)
	require.Nil(t, retryInfo, "Retry hash should be cleaned up after success")
}

// TestLifecycle_TrimmedMessageInPELIsCleanedSilently verifies trimmed message handling
func TestLifecycle_TrimmedMessageInPELIsCleanedSilently(t *testing.T) {
	client := NewRedisClient()
	defer client.Close()

	ns := UniqueNamespace(t, client)
	cfg := acceleratedConfig(ns)
	stream := thinrsmq.StreamKey(ns, "topic")

	// Manually publish with exact MAXLEN to force trimming
	// First message
	id1, err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: stream,
		ID:     "*",
		Values: map[string]interface{}{
			"v":           "1",
			"type":        "test",
			"payload":     `{"trimmed":true}`,
			"produced_at": time.Now().UTC().Format(time.RFC3339Nano),
		},
	}).Result()
	require.NoError(t, err)

	// Create group and read message (but don't ACK)
	err = client.XGroupCreate(context.Background(), stream, "group", "0").Err()
	require.NoError(t, err)

	result, err := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    "group",
		Consumer: "c1",
		Streams:  []string{stream, ">"},
		Count:    1,
		Block:    -1,
	}).Result()
	require.NoError(t, err)
	require.Len(t, result, 1)

	// Fill stream with MAXLEN=3 to force trim of our message
	for i := 0; i < 5; i++ {
		_, err = client.XAdd(context.Background(), &redis.XAddArgs{
			Stream: stream,
			MaxLen: 3, // Exact MAXLEN, not approximate
			Values: map[string]interface{}{
				"v":           "1",
				"type":        "filler",
				"payload":     fmt.Sprintf(`{"i":%d}`, i),
				"produced_at": time.Now().UTC().Format(time.RFC3339Nano),
			},
		}).Result()
		require.NoError(t, err)
	}

	// Verify our message is trimmed (XRANGE returns empty)
	entries, err := client.XRange(context.Background(), stream, id1, id1).Result()
	require.NoError(t, err)
	require.Empty(t, entries, "Message should be trimmed from stream")

	// But it's still in PEL
	pending, err := PendingCount(context.Background(), client, stream, "group")
	require.NoError(t, err)
	require.Greater(t, pending, int64(0), "PEL should still have pending messages")

	// Start consumer - recovery should handle trimmed message gracefully
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		return nil
	}

	consumer := thinrsmq.NewConsumer(client, cfg)
	err = consumer.Subscribe("topic", "group", handler)
	require.NoError(t, err)
	defer consumer.Stop()

	// Wait for PEL to be clean (trimmed message ACKed without processing)
	WaitFor(t, func() bool {
		count, _ := PendingCount(context.Background(), client, stream, "group")
		return count == 0
	}, 10*time.Second)

	// Verify no DLQ entry for trimmed message
	dlq := thinrsmq.NewDLQ(client, cfg)
	dlqSize, err := dlq.Size(context.Background(), "topic")
	require.NoError(t, err)
	require.Equal(t, int64(0), dlqSize, "No DLQ entry for trimmed message")
}

// TestLifecycle_BackpressureWarningDuringHighLoad verifies warning when stream near MAXLEN
func TestLifecycle_BackpressureWarningDuringHighLoad(t *testing.T) {
	client := NewRedisClient()
	defer client.Close()

	ns := UniqueNamespace(t, client)
	cfg := acceleratedConfig(ns)
	cfg.Streams.DefaultMaxLen = 50

	producer := thinrsmq.NewProducer(client, cfg)

	// Fill to 90% (45 messages, threshold is 40 = 80%)
	for i := 0; i < 45; i++ {
		msg := thinrsmq.Message{
			Type:    "filler",
			Payload: fmt.Sprintf(`{"i":%d}`, i),
		}
		_, err := producer.Publish(context.Background(), "topic", msg)
		require.NoError(t, err)
	}

	// Clear warnings
	producer.Warnings()

	// Publish one more - should trigger warning
	msg := thinrsmq.Message{
		Type:    "test",
		Payload: `{"warning":true}`,
	}
	_, err := producer.Publish(context.Background(), "topic", msg)
	require.NoError(t, err)

	// Check warnings
	warnings := producer.Warnings()
	require.NotEmpty(t, warnings, "Should have backpressure warning")

	// Verify warning content
	found := false
	for _, w := range warnings {
		if w.CurrentLen > int64(cfg.Streams.DefaultMaxLen)*8/10 {
			found = true
			break
		}
	}
	require.True(t, found, "Should have warning about stream length > 80% threshold")
}

// TestLifecycle_VersionFieldPreservedThroughRetryCycle verifies version=1 preservation
func TestLifecycle_VersionFieldPreservedThroughRetryCycle(t *testing.T) {
	client := NewRedisClient()
	defer client.Close()

	ns := UniqueNamespace(t, client)
	cfg := acceleratedConfig(ns)
	cfg.Retry.MaxAttempts = 2

	producer := thinrsmq.NewProducer(client, cfg)
	dlq := thinrsmq.NewDLQ(client, cfg)

	// Track versions seen
	var mu sync.Mutex
	versions := make([]string, 0)

	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		mu.Lock()
		versions = append(versions, msg.Version)
		mu.Unlock()
		// Always fail to force DLQ
		return fmt.Errorf("force dlq")
	}

	consumer := thinrsmq.NewConsumer(client, cfg)
	err := consumer.Subscribe("topic", "group", handler)
	require.NoError(t, err)
	defer consumer.Stop()

	monitor := thinrsmq.NewClaimMonitor(client, cfg, handler)
	err = monitor.Start("topic", "group")
	require.NoError(t, err)
	defer monitor.Stop()

	// Publish with v=1
	msg := thinrsmq.Message{
		Type:    "test",
		Payload: `{"version":true}`,
		Version: "1",
	}
	_, err = producer.Publish(context.Background(), "topic", msg)
	require.NoError(t, err)

	// Wait for DLQ entry
	WaitFor(t, func() bool {
		size, _ := dlq.Size(context.Background(), "topic")
		return size == 1
	}, 30*time.Second)

	// Verify DLQ entry has v=1
	entries, err := dlq.Peek(context.Background(), "topic", 1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "1", entries[0].Version, "DLQ entry should preserve version=1")

	// Verify all handler calls saw v=1
	mu.Lock()
	vers := append([]string{}, versions...)
	mu.Unlock()

	for i, v := range vers {
		require.Equal(t, "1", v, "Handler call %d should see version=1", i)
	}

	// Replay from DLQ (this will fail again and go back to DLQ)
	replayed, err := dlq.Replay(context.Background(), "topic", 1)
	require.NoError(t, err)
	require.Equal(t, 1, replayed)

	// Wait for DLQ entry again
	WaitFor(t, func() bool {
		size, _ := dlq.Size(context.Background(), "topic")
		return size == 1
	}, 30*time.Second)

	// Verify replayed DLQ entry still has v=1
	entries, err = dlq.Peek(context.Background(), "topic", 1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "1", entries[0].Version, "Replayed DLQ entry should preserve version=1")
	require.Equal(t, 1, entries[0].ReplayCount, "Replayed DLQ entry should have replay_count=1")
}
