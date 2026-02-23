package test

import (
	"context"
	"errors"
	"fmt"
	"github.com/your-org/thin-redis-queue/thinrsmq-go/pkg/thinrsmq"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupIdleMessage creates a message in the PEL for testing the monitor.
// Returns the message ID.
func setupIdleMessage(t *testing.T, client *redis.Client, ns, topic, group string) string {
	ctx := context.Background()
	stream := thinrsmq.StreamKey(ns, topic)

	// 1. Ensure group exists
	err := client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		require.NoError(t, err)
	}

	// 2. Publish a message
	msg := thinrsmq.Message{
		Type:    "test",
		Payload: `{"test":true}`,
	}
	fields := msg.ToStreamFields()
	id, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		ID:     "*",
		Values: fields,
	}).Result()
	require.NoError(t, err)

	// 3. Read it into PEL of a throwaway consumer
	_, err = client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: "throwaway-consumer",
		Streams:  []string{stream, ">"},
		Count:    1,
		Block:    -1, // Non-blocking
	}).Result()
	require.NoError(t, err)

	return id
}

func TestMonitor_DetectsIdleMessageViaXPending(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Monitor.MinIdleMs = 1000 // 1 second
	cfg.Monitor.BatchSize = 10
	cfg = cfg.WithDefaults()

	id := setupIdleMessage(t, client, ns, "topic", "group")

	// Set retry hash with expired backoff
	retryStore := thinrsmq.NewRetryStore(client, cfg)
	now := time.Now().UTC().UnixMilli()
	err := retryStore.Set(ctx, "topic", id, thinrsmq.RetryInfo{
		Attempt:       1,
		MaxAttempts:   5,
		NextRetryAt:   now - 1000, // expired
		LastError:     "test error",
		FirstFailedAt: now - 5000,
	})
	require.NoError(t, err)

	// Sleep to ensure message is idle
	time.Sleep(1500 * time.Millisecond)

	// Create monitor
	processed := atomic.Int32{}
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		processed.Add(1)
		return nil
	}

	monitor := thinrsmq.NewClaimMonitor(client, cfg, handler)
	count, err := monitor.ScanOnce(ctx, "topic", "group")

	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, 1, "should process at least 1 message")
	assert.Equal(t, int32(1), processed.Load(), "handler should be called once")
}

func TestMonitor_SkipsMessageWhenBackoffNotElapsed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Monitor.MinIdleMs = 1000
	cfg.Monitor.BatchSize = 10
	cfg = cfg.WithDefaults()

	id := setupIdleMessage(t, client, ns, "topic", "group")

	// Set retry hash with future backoff
	retryStore := thinrsmq.NewRetryStore(client, cfg)
	now := time.Now().UTC().UnixMilli()
	err := retryStore.Set(ctx, "topic", id, thinrsmq.RetryInfo{
		Attempt:       1,
		MaxAttempts:   5,
		NextRetryAt:   now + 60000, // 60 seconds in future
		LastError:     "test error",
		FirstFailedAt: now - 5000,
	})
	require.NoError(t, err)

	// Sleep to ensure message is idle
	time.Sleep(1500 * time.Millisecond)

	processed := atomic.Int32{}
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		processed.Add(1)
		return nil
	}

	monitor := thinrsmq.NewClaimMonitor(client, cfg, handler)
	count, err := monitor.ScanOnce(ctx, "topic", "group")

	require.NoError(t, err)
	assert.Equal(t, int32(0), processed.Load(), "handler should NOT be called (backoff not elapsed)")
	assert.Equal(t, 0, count, "no messages should be processed")
}

func TestMonitor_ClaimsAndReprocessesWhenBackoffElapsed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Monitor.MinIdleMs = 1000
	cfg.Monitor.BatchSize = 10
	cfg = cfg.WithDefaults()

	id := setupIdleMessage(t, client, ns, "topic", "group")

	// Set retry hash with expired backoff
	retryStore := thinrsmq.NewRetryStore(client, cfg)
	now := time.Now().UTC().UnixMilli()
	err := retryStore.Set(ctx, "topic", id, thinrsmq.RetryInfo{
		Attempt:       1,
		MaxAttempts:   5,
		NextRetryAt:   now - 1000, // expired
		LastError:     "test error",
		FirstFailedAt: now - 5000,
	})
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	var receivedMsg *thinrsmq.Message
	var mu sync.Mutex
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		mu.Lock()
		receivedMsg = msg
		mu.Unlock()
		return nil
	}

	monitor := thinrsmq.NewClaimMonitor(client, cfg, handler)
	_, err = monitor.ScanOnce(ctx, "topic", "group")
	require.NoError(t, err)

	mu.Lock()
	assert.NotNil(t, receivedMsg, "message should be received")
	if receivedMsg != nil {
		assert.Equal(t, id, receivedMsg.ID)
	}
	mu.Unlock()
}

func TestMonitor_SuccessfulReprocessACKsAndCleansRetryHash(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Monitor.MinIdleMs = 1000
	cfg.Monitor.BatchSize = 10
	cfg = cfg.WithDefaults()

	id := setupIdleMessage(t, client, ns, "topic", "group")

	retryStore := thinrsmq.NewRetryStore(client, cfg)
	now := time.Now().UTC().UnixMilli()
	err := retryStore.Set(ctx, "topic", id, thinrsmq.RetryInfo{
		Attempt:       1,
		MaxAttempts:   5,
		NextRetryAt:   now - 1000,
		LastError:     "test error",
		FirstFailedAt: now - 5000,
	})
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		return nil // success
	}

	monitor := thinrsmq.NewClaimMonitor(client, cfg, handler)
	_, err = monitor.ScanOnce(ctx, "topic", "group")
	require.NoError(t, err)

	// Verify message ACKed
	stream := thinrsmq.StreamKey(ns, "topic")
	pendingCount, err := PendingCount(ctx, client, stream, "group")
	require.NoError(t, err)
	assert.Equal(t, int64(0), pendingCount, "message should be ACKed")

	// Verify retry hash deleted
	info, err := retryStore.Get(ctx, "topic", id)
	require.NoError(t, err)
	assert.Nil(t, info, "retry hash should be deleted")
}

func TestMonitor_FailedReprocessIncrementsAttemptAndSetsNewBackoff(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Monitor.MinIdleMs = 1000
	cfg.Monitor.BatchSize = 10
	cfg = cfg.WithDefaults()

	id := setupIdleMessage(t, client, ns, "topic", "group")

	retryStore := thinrsmq.NewRetryStore(client, cfg)
	now := time.Now().UTC().UnixMilli()
	firstFailedAt := now - 5000
	err := retryStore.Set(ctx, "topic", id, thinrsmq.RetryInfo{
		Attempt:       1,
		MaxAttempts:   5,
		NextRetryAt:   now - 1000,
		LastError:     "old error",
		FirstFailedAt: firstFailedAt,
	})
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		return errors.New("still broken")
	}

	monitor := thinrsmq.NewClaimMonitor(client, cfg, handler)
	_, err = monitor.ScanOnce(ctx, "topic", "group")
	require.NoError(t, err)

	// Verify attempt incremented
	info, err := retryStore.Get(ctx, "topic", id)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, 2, info.Attempt, "attempt should be incremented to 2")
	assert.Greater(t, info.NextRetryAt, now, "next retry should be in future")
	assert.Equal(t, "still broken", info.LastError, "last error should be updated")
	assert.Equal(t, firstFailedAt, info.FirstFailedAt, "first failed at should be unchanged")
}

func TestMonitor_MovesMessageToDLQWhenMaxAttemptsReached(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Monitor.MinIdleMs = 1000
	cfg.Monitor.BatchSize = 10
	cfg = cfg.WithDefaults()

	id := setupIdleMessage(t, client, ns, "topic", "group")

	retryStore := thinrsmq.NewRetryStore(client, cfg)
	now := time.Now().UTC().UnixMilli()
	err := retryStore.Set(ctx, "topic", id, thinrsmq.RetryInfo{
		Attempt:       5,
		MaxAttempts:   5,
		NextRetryAt:   now - 1000,
		LastError:     "final error",
		FirstFailedAt: now - 10000,
	})
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	monitor := thinrsmq.NewClaimMonitor(client, cfg, nil)
	_, err = monitor.ScanOnce(ctx, "topic", "group")
	require.NoError(t, err)

	// Verify DLQ entry exists
	dlqStream := thinrsmq.DLQKey(ns, "topic")
	dlqLen, err := client.XLen(ctx, dlqStream).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), dlqLen, "DLQ should have 1 entry")

	// Verify DLQ entry fields
	entries, err := client.XRange(ctx, dlqStream, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)

	dlqEntry := entries[0]
	assert.Equal(t, id, dlqEntry.Values["original_id"])
	assert.Equal(t, "5", dlqEntry.Values["total_attempts"])
	assert.Equal(t, "final error", dlqEntry.Values["last_error"])
	assert.Equal(t, "0", dlqEntry.Values["replay_count"])

	// Verify cleanup
	stream := thinrsmq.StreamKey(ns, "topic")
	pendingCount, err := PendingCount(ctx, client, stream, "group")
	require.NoError(t, err)
	assert.Equal(t, int64(0), pendingCount, "message should be ACKed")

	info, err := retryStore.Get(ctx, "topic", id)
	require.NoError(t, err)
	assert.Nil(t, info, "retry hash should be deleted")
}

func TestMonitor_HandlesTrimmedMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Monitor.MinIdleMs = 1000
	cfg.Monitor.BatchSize = 10
	cfg = cfg.WithDefaults()

	stream := thinrsmq.StreamKey(ns, "topic")

	// Create group
	err := client.XGroupCreateMkStream(ctx, stream, "group", "0").Err()
	require.NoError(t, err)

	// Publish message
	p := thinrsmq.NewProducer(client, cfg)
	msg := thinrsmq.Message{Type: "test", Payload: `{"x":1}`}
	id, err := p.Publish(ctx, "topic", msg)
	require.NoError(t, err)

	// Read into PEL
	_, err = client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "group",
		Consumer: "c1",
		Streams:  []string{stream, ">"},
		Count:    1,
		Block:    -1,
	}).Result()
	require.NoError(t, err)

	// Delete the message (simulate trimming)
	err = client.XDel(ctx, stream, id).Err()
	require.NoError(t, err)

	// Set retry hash as if message exhausted retries
	retryStore := thinrsmq.NewRetryStore(client, cfg)
	now := time.Now().UTC().UnixMilli()
	err = retryStore.Set(ctx, "topic", id, thinrsmq.RetryInfo{
		Attempt:       5,
		MaxAttempts:   5,
		NextRetryAt:   now - 1000,
		LastError:     "final",
		FirstFailedAt: now - 10000,
	})
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	processCount := atomic.Int32{}
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		processCount.Add(1)
		return nil
	}

	monitor := thinrsmq.NewClaimMonitor(client, cfg, handler)
	_, err = monitor.ScanOnce(ctx, "topic", "group")
	require.NoError(t, err)

	// Trimmed message: should NOT be processed, should be cleaned up
	assert.Equal(t, int32(0), processCount.Load(), "handler should not be called for trimmed message")
	pendingCount, err := PendingCount(ctx, client, stream, "group")
	require.NoError(t, err)
	assert.Equal(t, int64(0), pendingCount, "message should be ACKed")

	info, err := retryStore.Get(ctx, "topic", id)
	require.NoError(t, err)
	assert.Nil(t, info, "retry hash should be deleted")

	// Should NOT write to DLQ (nothing to write)
	dlqStream := thinrsmq.DLQKey(ns, "topic")
	dlqLen, err := client.XLen(ctx, dlqStream).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), dlqLen, "DLQ should be empty")
}

func TestMonitor_ConcurrentMonitorsOnlyProcessMessageOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Monitor.MinIdleMs = 1000
	cfg.Monitor.BatchSize = 10
	cfg = cfg.WithDefaults()

	id := setupIdleMessage(t, client, ns, "topic", "group")

	retryStore := thinrsmq.NewRetryStore(client, cfg)
	now := time.Now().UTC().UnixMilli()
	err := retryStore.Set(ctx, "topic", id, thinrsmq.RetryInfo{
		Attempt:       1,
		MaxAttempts:   5,
		NextRetryAt:   now - 1000,
		LastError:     "test",
		FirstFailedAt: now - 5000,
	})
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	processCount := atomic.Int32{}
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		processCount.Add(1)
		time.Sleep(100 * time.Millisecond) // Slow handler
		return nil
	}

	monitor1 := thinrsmq.NewClaimMonitor(client, cfg, handler)
	monitor2 := thinrsmq.NewClaimMonitor(client, cfg, handler)

	// Run concurrently
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		monitor1.ScanOnce(ctx, "topic", "group")
	}()

	go func() {
		defer wg.Done()
		monitor2.ScanOnce(ctx, "topic", "group")
	}()

	wg.Wait()

	// Wait for PEL to clear
	stream := thinrsmq.StreamKey(ns, "topic")
	WaitFor(t, func() bool {
		pc, _ := PendingCount(ctx, client, stream, "group")
		return pc == 0
	}, 5*time.Second)

	// Only one monitor should have won the XCLAIM
	assert.Equal(t, int32(1), processCount.Load(), "message should be processed exactly once")
}

func TestMonitor_UsesXClaimWithExplicitIDs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Monitor.MinIdleMs = 1000
	cfg.Monitor.BatchSize = 10
	cfg = cfg.WithDefaults()

	id := setupIdleMessage(t, client, ns, "topic", "group")

	retryStore := thinrsmq.NewRetryStore(client, cfg)
	now := time.Now().UTC().UnixMilli()
	err := retryStore.Set(ctx, "topic", id, thinrsmq.RetryInfo{
		Attempt:       1,
		MaxAttempts:   5,
		NextRetryAt:   now - 1000,
		LastError:     "test",
		FirstFailedAt: now - 5000,
	})
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	}

	monitor := thinrsmq.NewClaimMonitor(client, cfg, handler)
	_, err = monitor.ScanOnce(ctx, "topic", "group")
	require.NoError(t, err)

	// Behavioral verification: after thinrsmq.ScanOnce, the message should be processed and PEL clean
	stream := thinrsmq.StreamKey(ns, "topic")
	pendingCount, err := PendingCount(ctx, client, stream, "group")
	require.NoError(t, err)
	assert.Equal(t, int64(0), pendingCount, "message should be claimed and processed")
}

func TestMonitor_DoesNotReinitExistingHash(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Monitor.MinIdleMs = 1000
	cfg.Monitor.BatchSize = 10
	cfg = cfg.WithDefaults()

	id := setupIdleMessage(t, client, ns, "topic", "group")

	// Hash exists with attempt=3
	retryStore := thinrsmq.NewRetryStore(client, cfg)
	now := time.Now().UTC().UnixMilli()
	err := retryStore.Set(ctx, "topic", id, thinrsmq.RetryInfo{
		Attempt:       3,
		MaxAttempts:   5,
		NextRetryAt:   now - 1000,
		LastError:     "old error",
		FirstFailedAt: now - 5000,
	})
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		return errors.New("still failing")
	}

	monitor := thinrsmq.NewClaimMonitor(client, cfg, handler)
	_, err = monitor.ScanOnce(ctx, "topic", "group")
	require.NoError(t, err)

	// Monitor should increment to 4, NOT reset to 1
	info, err := retryStore.Get(ctx, "topic", id)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, 4, info.Attempt, "attempt should be incremented to 4, not reset")
	assert.Equal(t, "still failing", info.LastError)
}

func TestMonitor_ConsumerAndMonitorNoDoubleIncrement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Monitor.MinIdleMs = 1000
	cfg.Monitor.BatchSize = 10
	cfg = cfg.WithDefaults()

	id := setupIdleMessage(t, client, ns, "topic", "group")

	// Consumer inits hash (attempt=1) with future backoff
	retryStore := thinrsmq.NewRetryStore(client, cfg)
	now := time.Now().UTC().UnixMilli()
	_, err := retryStore.InitIfNotExists(ctx, "topic", id, "consumer init error")
	require.NoError(t, err)

	info, err := retryStore.Get(ctx, "topic", id)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, 1, info.Attempt)

	// Ensure backoff is NOT elapsed
	err = retryStore.Set(ctx, "topic", id, thinrsmq.RetryInfo{
		Attempt:       1,
		MaxAttempts:   5,
		NextRetryAt:   now + 60000, // far in future
		LastError:     "consumer init error",
		FirstFailedAt: now,
	})
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	// Monitor runs -- sees hash exists, attempt=1, but backoff NOT elapsed
	monitor := thinrsmq.NewClaimMonitor(client, cfg, nil)
	_, err = monitor.ScanOnce(ctx, "topic", "group")
	require.NoError(t, err)

	// Attempt should still be 1 (monitor skipped because backoff pending)
	info, err = retryStore.Get(ctx, "topic", id)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, 1, info.Attempt, "attempt should remain 1 (monitor skipped due to backoff)")
}

func TestMonitor_UsesHashAttemptNotTimesDelivered(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Monitor.MinIdleMs = 1000
	cfg.Monitor.BatchSize = 10
	cfg = cfg.WithDefaults()

	id := setupIdleMessage(t, client, ns, "topic", "group")
	stream := thinrsmq.StreamKey(ns, "topic")

	// Deliver multiple times via XCLAIM (simulating crashes that increment times_delivered)
	for i := 1; i <= 4; i++ {
		_, err := client.XClaim(ctx, &redis.XClaimArgs{
			Stream:   stream,
			Group:    "group",
			Consumer: fmt.Sprintf("dummy-%d", i),
			MinIdle:  0,
			Messages: []string{id},
		}).Result()
		require.NoError(t, err)
	}

	// Set retry hash at attempt=2 (not 5)
	retryStore := thinrsmq.NewRetryStore(client, cfg)
	now := time.Now().UTC().UnixMilli()
	err := retryStore.Set(ctx, "topic", id, thinrsmq.RetryInfo{
		Attempt:       2,
		MaxAttempts:   5,
		NextRetryAt:   now - 1000,
		LastError:     "fail",
		FirstFailedAt: now - 5000,
	})
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		return errors.New("fail again")
	}

	monitor := thinrsmq.NewClaimMonitor(client, cfg, handler)
	_, err = monitor.ScanOnce(ctx, "topic", "group")
	require.NoError(t, err)

	// Monitor should increment hash attempt to 3, not use PEL's times_delivered
	info, err := retryStore.Get(ctx, "topic", id)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, 3, info.Attempt, "attempt should be 3 (based on hash, not times_delivered)")
}

func TestMonitor_PipelinesACKAndDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Monitor.MinIdleMs = 1000
	cfg.Monitor.BatchSize = 10
	cfg = cfg.WithDefaults()

	id := setupIdleMessage(t, client, ns, "topic", "group")

	retryStore := thinrsmq.NewRetryStore(client, cfg)
	now := time.Now().UTC().UnixMilli()
	err := retryStore.Set(ctx, "topic", id, thinrsmq.RetryInfo{
		Attempt:       1,
		MaxAttempts:   5,
		NextRetryAt:   now - 1000,
		LastError:     "test",
		FirstFailedAt: now - 5000,
	})
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		return nil // success
	}

	monitor := thinrsmq.NewClaimMonitor(client, cfg, handler)
	_, err = monitor.ScanOnce(ctx, "topic", "group")
	require.NoError(t, err)

	// Behavioral verification: both XACK and DEL happened
	stream := thinrsmq.StreamKey(ns, "topic")
	pendingCount, err := PendingCount(ctx, client, stream, "group")
	require.NoError(t, err)
	assert.Equal(t, int64(0), pendingCount, "message should be ACKed")

	info, err := retryStore.Get(ctx, "topic", id)
	require.NoError(t, err)
	assert.Nil(t, info, "retry hash should be deleted")
}

func TestMonitor_PipelinesDLQMove(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Monitor.MinIdleMs = 1000
	cfg.Monitor.BatchSize = 10
	cfg = cfg.WithDefaults()

	id := setupIdleMessage(t, client, ns, "topic", "group")

	retryStore := thinrsmq.NewRetryStore(client, cfg)
	now := time.Now().UTC().UnixMilli()
	err := retryStore.Set(ctx, "topic", id, thinrsmq.RetryInfo{
		Attempt:       5,
		MaxAttempts:   5,
		NextRetryAt:   now - 1000,
		LastError:     "exhausted",
		FirstFailedAt: now - 10000,
	})
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	monitor := thinrsmq.NewClaimMonitor(client, cfg, nil)
	_, err = monitor.ScanOnce(ctx, "topic", "group")
	require.NoError(t, err)

	// All three operations should have completed atomically (in pipeline):
	// 1. XADD DLQ
	dlqStream := thinrsmq.DLQKey(ns, "topic")
	dlqLen, err := client.XLen(ctx, dlqStream).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), dlqLen, "DLQ should have 1 entry")

	// 2. XACK main
	stream := thinrsmq.StreamKey(ns, "topic")
	pendingCount, err := PendingCount(ctx, client, stream, "group")
	require.NoError(t, err)
	assert.Equal(t, int64(0), pendingCount, "message should be ACKed")

	// 3. DEL retry hash
	info, err := retryStore.Get(ctx, "topic", id)
	require.NoError(t, err)
	assert.Nil(t, info, "retry hash should be deleted")
}

func TestMonitor_DuplicateDLQEntryOnCrashIsHarmless(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Monitor.MinIdleMs = 1000
	cfg.Monitor.BatchSize = 10
	cfg = cfg.WithDefaults()

	id := setupIdleMessage(t, client, ns, "topic", "group")
	stream := thinrsmq.StreamKey(ns, "topic")
	dlqStream := thinrsmq.DLQKey(ns, "topic")

	// Simulate: DLQ already has an entry for this original_id
	// (from a previous crashed pipeline)
	_, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: dlqStream,
		ID:     "*",
		Values: map[string]interface{}{
			"original_id":     id,
			"original_stream": stream,
			"v":               "1",
			"type":            "test",
			"payload":         `{"test":true}`,
			"failed_at":       time.Now().UTC().Format(time.RFC3339Nano),
			"total_attempts":  "5",
			"last_error":      "prev crash",
			"consumer_group":  "group",
			"replay_count":    "0",
		},
	}).Result()
	require.NoError(t, err)

	dlqLenBefore, err := client.XLen(ctx, dlqStream).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), dlqLenBefore)

	// Now monitor processes same message (still in PEL because XACK didn't run)
	retryStore := thinrsmq.NewRetryStore(client, cfg)
	now := time.Now().UTC().UnixMilli()
	err = retryStore.Set(ctx, "topic", id, thinrsmq.RetryInfo{
		Attempt:       5,
		MaxAttempts:   5,
		NextRetryAt:   now - 1000,
		LastError:     "exhausted",
		FirstFailedAt: now - 10000,
	})
	require.NoError(t, err)

	time.Sleep(1500 * time.Millisecond)

	monitor := thinrsmq.NewClaimMonitor(client, cfg, nil)
	_, err = monitor.ScanOnce(ctx, "topic", "group")
	require.NoError(t, err)

	dlqLenAfter, err := client.XLen(ctx, dlqStream).Result()
	require.NoError(t, err)
	// Duplicate DLQ entry is present but harmless
	assert.Equal(t, dlqLenBefore+1, dlqLenAfter, "should have 2 entries (duplicate)")

	// Both entries have same original_id
	entries, err := client.XRange(ctx, dlqStream, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, id, entries[0].Values["original_id"])
	assert.Equal(t, id, entries[1].Values["original_id"])
}

func TestMonitor_HandlesNoRetryHashCrashScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Monitor.MinIdleMs = 1000
	cfg.Monitor.BatchSize = 10
	cfg = cfg.WithDefaults()

	stream := thinrsmq.StreamKey(ns, "topic")

	// Create group
	err := client.XGroupCreateMkStream(ctx, stream, "group", "0").Err()
	require.NoError(t, err)

	// Publish message
	p := thinrsmq.NewProducer(client, cfg)
	msg := thinrsmq.Message{Type: "test", Payload: `{"x":1}`}
	id, err := p.Publish(ctx, "topic", msg)
	require.NoError(t, err)

	// Read into PEL (simulate consumer crash before writing retry hash)
	_, err = client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "group",
		Consumer: "c1",
		Streams:  []string{stream, ">"},
		Count:    1,
		Block:    -1,
	}).Result()
	require.NoError(t, err)

	// No retry hash exists
	time.Sleep(1500 * time.Millisecond)

	monitor := thinrsmq.NewClaimMonitor(client, cfg, nil)
	_, err = monitor.ScanOnce(ctx, "topic", "group")
	require.NoError(t, err)

	// Monitor should have initialized retry hash
	retryStore := thinrsmq.NewRetryStore(client, cfg)
	info, err := retryStore.Get(ctx, "topic", id)
	require.NoError(t, err)
	require.NotNil(t, info, "retry hash should be initialized by monitor")
	assert.Equal(t, 1, info.Attempt)
	assert.Contains(t, info.LastError, "consumer did not ACK")

	// Message stays in PEL -- will be retried on next cycle after backoff
	pendingCount, err := PendingCount(ctx, client, stream, "group")
	require.NoError(t, err)
	assert.Equal(t, int64(1), pendingCount, "message should remain in PEL")
}
