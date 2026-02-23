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

func TestConsumer_SubscribeCreatesGroup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg = cfg.WithDefaults()

	c := thinrsmq.NewConsumer(client, cfg)

	handlerCalled := atomic.Bool{}
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		handlerCalled.Store(true)
		return nil
	}

	err := c.Subscribe("test-topic", "test-group", handler)
	require.NoError(t, err)

	defer c.Stop()

	// Verify group was created
	stream := thinrsmq.StreamKey(ns, "test-topic")
	groups, err := client.XInfoGroups(ctx, stream).Result()
	require.NoError(t, err)

	found := false
	for _, g := range groups {
		if g.Name == "test-group" {
			found = true
			break
		}
	}
	assert.True(t, found, "group should be created")
}

func TestConsumer_SubscribeExistingGroupNoError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg = cfg.WithDefaults()

	stream := thinrsmq.StreamKey(ns, "test-topic")

	// Create group explicitly first
	err := client.XGroupCreateMkStream(ctx, stream, "test-group", "0").Err()
	require.NoError(t, err)

	// Subscribe should NOT error
	c := thinrsmq.NewConsumer(client, cfg)
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		return nil
	}

	err = c.Subscribe("test-topic", "test-group", handler)
	assert.NoError(t, err, "subscribing to existing group should not error")

	defer c.Stop()

	// Verify group still exists
	groups, err := client.XInfoGroups(ctx, stream).Result()
	require.NoError(t, err)

	found := false
	for _, g := range groups {
		if g.Name == "test-group" {
			found = true
			break
		}
	}
	assert.True(t, found)
}

func TestConsumer_ReceivesPublishedMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg = cfg.WithDefaults()

	// Channel to receive message
	received := make(chan *thinrsmq.Message, 1)
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		received <- msg
		return nil
	}

	c := thinrsmq.NewConsumer(client, cfg)
	err := c.Subscribe("test-topic", "test-group", handler)
	require.NoError(t, err)

	defer c.Stop()

	// Publish a message
	p := thinrsmq.NewProducer(client, cfg)
	msg := thinrsmq.Message{
		Type:    "hello",
		Payload: `{"x":1}`,
	}
	_, err = p.Publish(ctx, "test-topic", msg)
	require.NoError(t, err)

	// Wait for message
	select {
	case receivedMsg := <-received:
		assert.Equal(t, "hello", receivedMsg.Type)
		assert.Equal(t, `{"x":1}`, receivedMsg.Payload)
		assert.Equal(t, "1", receivedMsg.Version)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestConsumer_SuccessfulHandlerACKs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg = cfg.WithDefaults()

	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		return nil // always succeed
	}

	c := thinrsmq.NewConsumer(client, cfg)
	err := c.Subscribe("test-topic", "test-group", handler)
	require.NoError(t, err)

	defer c.Stop()

	// Publish a message
	p := thinrsmq.NewProducer(client, cfg)
	msg := thinrsmq.Message{Type: "test", Payload: "{}"}
	_, err = p.Publish(ctx, "test-topic", msg)
	require.NoError(t, err)

	// Wait for processing
	stream := thinrsmq.StreamKey(ns, "test-topic")
	WaitFor(t, func() bool {
		count, _ := PendingCount(ctx, client, stream, "test-group")
		return count == 0
	}, 5*time.Second)
}

func TestConsumer_FailedHandlerLeavesInPEL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg = cfg.WithDefaults()

	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		return errors.New("handler failed")
	}

	c := thinrsmq.NewConsumer(client, cfg)
	err := c.Subscribe("test-topic", "test-group", handler)
	require.NoError(t, err)

	defer c.Stop()

	// Publish a message
	p := thinrsmq.NewProducer(client, cfg)
	msg := thinrsmq.Message{Type: "test", Payload: "{}"}
	_, err = p.Publish(ctx, "test-topic", msg)
	require.NoError(t, err)

	// Wait a bit for processing
	time.Sleep(1 * time.Second)

	// Check pending count
	stream := thinrsmq.StreamKey(ns, "test-topic")
	pending, err := client.XPending(ctx, stream, "test-group").Result()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, pending.Count, int64(1), "message should stay in PEL")
}

func TestConsumer_RecoveryProcessesPending(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg = cfg.WithDefaults()

	stream := thinrsmq.StreamKey(ns, "test-topic")

	// Setup: create group, publish, read into thinrsmq.PEL, don't ACK
	err := client.XGroupCreateMkStream(ctx, stream, "test-group", "0").Err()
	require.NoError(t, err)

	p := thinrsmq.NewProducer(client, cfg)
	msg := thinrsmq.Message{Type: "test", Payload: "{}"}
	_, err = p.Publish(ctx, "test-topic", msg)
	require.NoError(t, err)

	// Read into PEL of old consumer without ACKing
	_, err = client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "test-group",
		Consumer: "old-consumer",
		Streams:  []string{stream, ">"},
		Count:    1,
	}).Result()
	require.NoError(t, err)

	// Verify message is in PEL
	pending, err := client.XPending(ctx, stream, "test-group").Result()
	require.NoError(t, err)
	require.GreaterOrEqual(t, pending.Count, int64(1))

	// New consumer with same group
	received := make(chan *thinrsmq.Message, 1)
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		received <- msg
		return nil
	}

	c := thinrsmq.NewConsumer(client, cfg)
	err = c.Subscribe("test-topic", "test-group", handler)
	require.NoError(t, err)

	defer c.Stop()

	// Wait for recovery
	select {
	case receivedMsg := <-received:
		assert.NotNil(t, receivedMsg, "should recover from PEL")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for recovery")
	}

	// Wait for ACK
	WaitFor(t, func() bool {
		count, _ := PendingCount(ctx, client, stream, "test-group")
		return count == 0
	}, 5*time.Second)
}

func TestConsumer_StopFinishesInFlightMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg = cfg.WithDefaults()

	started := make(chan bool, 1)
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		started <- true
		time.Sleep(2 * time.Second) // simulate slow processing
		return nil
	}

	c := thinrsmq.NewConsumer(client, cfg)
	err := c.Subscribe("test-topic", "test-group", handler)
	require.NoError(t, err)

	// Publish a message
	p := thinrsmq.NewProducer(client, cfg)
	msg := thinrsmq.Message{Type: "test", Payload: "{}"}
	_, err = p.Publish(ctx, "test-topic", msg)
	require.NoError(t, err)

	// Wait until handler is running
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not start")
	}

	// Stop should block until handler finishes
	err = c.Stop()
	assert.NoError(t, err)

	// Message should be ACKed
	stream := thinrsmq.StreamKey(ns, "test-topic")
	pending, err := client.XPending(ctx, stream, "test-group").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count, "message should be ACKed")
}

func TestConsumer_RecoveryReadsBoundedBatches(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Consumer.BatchSize = 10
	cfg = cfg.WithDefaults()

	stream := thinrsmq.StreamKey(ns, "test-topic")

	// Pre-populate: 250 messages in PEL
	err := client.XGroupCreateMkStream(ctx, stream, "test-group", "0").Err()
	require.NoError(t, err)

	p := thinrsmq.NewProducer(client, cfg)
	for i := 0; i < 250; i++ {
		msg := thinrsmq.Message{Type: "test", Payload: fmt.Sprintf(`{"i":%d}`, i)}
		_, err := p.Publish(ctx, "test-topic", msg)
		require.NoError(t, err)
	}

	// Read all into PEL of throwaway consumer
	for {
		result, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    "test-group",
			Consumer: "old-consumer",
			Streams:  []string{stream, ">"},
			Count:    50,
			Block:    -1, // Negative value = non-blocking (return immediately if no messages)
		}).Result()

		// redis.Nil means no more messages - exit loop
		if err == redis.Nil {
			break
		}
		require.NoError(t, err)

		if len(result) == 0 || len(result[0].Messages) == 0 {
			break
		}
	}

	// New consumer with batch_size=10
	var processedIDs sync.Map
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		processedIDs.Store(msg.ID, true)
		return nil
	}

	c := thinrsmq.NewConsumer(client, cfg)
	err = c.Subscribe("test-topic", "test-group", handler)
	require.NoError(t, err)

	defer c.Stop()

	// Wait for all 250 to be processed
	WaitFor(t, func() bool {
		count, _ := PendingCount(ctx, client, stream, "test-group")
		return count == 0
	}, 30*time.Second)

	// Count processed messages
	count := 0
	processedIDs.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	assert.Equal(t, 250, count, "should process all 250 messages")
}

func TestConsumer_ACKsNilMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg = cfg.WithDefaults()

	stream := thinrsmq.StreamKey(ns, "test-topic")

	// Publish, read into thinrsmq.PEL, then XDEL (simulates trimming)
	p := thinrsmq.NewProducer(client, cfg)
	msg := thinrsmq.Message{Type: "test", Payload: "{}"}
	id, err := p.Publish(ctx, "test-topic", msg)
	require.NoError(t, err)

	err = client.XGroupCreateMkStream(ctx, stream, "test-group", "0").Err()
	require.NoError(t, err)

	_, err = client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "test-group",
		Consumer: "old-consumer",
		Streams:  []string{stream, ">"},
		Count:    1,
	}).Result()
	require.NoError(t, err)

	// Delete the message (simulate trimming)
	err = client.XDel(ctx, stream, id).Err()
	require.NoError(t, err)

	// Start consumer
	handlerCalled := atomic.Bool{}
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		handlerCalled.Store(true)
		return nil
	}

	c := thinrsmq.NewConsumer(client, cfg)
	err = c.Subscribe("test-topic", "test-group", handler)
	require.NoError(t, err)

	defer c.Stop()

	time.Sleep(2 * time.Second)

	// Handler should NOT be called for nil message
	assert.False(t, handlerCalled.Load(), "handler should not be called for nil message")

	// Nil message should be ACKed
	pending, err := client.XPending(ctx, stream, "test-group").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count, "nil message should be ACKed")
}

func TestConsumer_NameIncludesUniqueSuffix(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg = cfg.WithDefaults()

	c1 := thinrsmq.NewConsumer(client, cfg)
	c2 := thinrsmq.NewConsumer(client, cfg)

	name1 := c1.ConsumerName()
	name2 := c2.ConsumerName()

	assert.NotEqual(t, name1, name2, "consumer names should be unique")

	// Verify format contains uuid-like suffix
	assert.Regexp(t, `.*-[a-f0-9]{8}$`, name1, "consumer name should end with 8-char hex")
	assert.Regexp(t, `.*-[a-f0-9]{8}$`, name2, "consumer name should end with 8-char hex")
}

func TestConsumer_RejectsUnknownVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg = cfg.WithDefaults()

	stream := thinrsmq.StreamKey(ns, "test-topic")

	// Publish message with v=99 directly via XADD
	_, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		ID:     "*",
		Values: map[string]interface{}{
			"v":       "99",
			"type":    "test",
			"payload": "{}",
		},
	}).Result()
	require.NoError(t, err)

	skipped := atomic.Bool{}
	handlerCalled := atomic.Bool{}
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		handlerCalled.Store(true)
		return nil
	}

	c := thinrsmq.NewConsumer(client, cfg)
	c.OnSkip(func(id string, reason string) {
		skipped.Store(true)
	})

	err = c.Subscribe("test-topic", "test-group", handler)
	require.NoError(t, err)

	defer c.Stop()

	// Wait for processing
	time.Sleep(2 * time.Second)

	assert.True(t, skipped.Load(), "message should be skipped")
	assert.False(t, handlerCalled.Load(), "handler should not be called")

	// Message should be ACKed (removed from thinrsmq.PEL)
	pending, err := client.XPending(ctx, stream, "test-group").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count, "message should be ACKed")
}
