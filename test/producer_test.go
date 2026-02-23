package test

import (
	"context"
	"github.com/your-org/thin-redis-queue/thinrsmq-go/pkg/thinrsmq"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test 1: publish_returns_valid_stream_id
func TestProducer_PublishReturnsValidStreamID(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg = cfg.WithDefaults()

	p := thinrsmq.NewProducer(client, cfg)

	msg := thinrsmq.Message{
		Type:    "test",
		Payload: `{"key":"value"}`,
	}

	id, err := p.Publish(ctx, "test-topic", msg)
	require.NoError(t, err)

	// Verify ID matches Redis stream ID format: {ms}-{seq}
	matched, _ := regexp.MatchString(`^\d+-\d+$`, id)
	assert.True(t, matched, "ID should match pattern {ms}-{seq}, got: %s", id)
}

// Test 2: publish_auto_creates_stream
func TestProducer_PublishAutoCreatesStream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg = cfg.WithDefaults()

	p := thinrsmq.NewProducer(client, cfg)

	msg := thinrsmq.Message{
		Type:    "brand-new",
		Payload: `{}`,
	}

	_, err := p.Publish(ctx, "brand-new-topic", msg)
	require.NoError(t, err)

	streamKey := thinrsmq.StreamKey(ns, "brand-new-topic")
	exists, err := client.Exists(ctx, streamKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), exists)
}

// Test 3: published_message_readable_with_xrange
func TestProducer_PublishedMessageReadableWithXRange(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg = cfg.WithDefaults()

	p := thinrsmq.NewProducer(client, cfg)

	msg := thinrsmq.Message{
		Type:     "test-type",
		Payload:  `{"k":"v"}`,
		TraceID:  "trace123",
		Producer: "test-service",
	}

	id, err := p.Publish(ctx, "topic", msg)
	require.NoError(t, err)

	streamKey := thinrsmq.StreamKey(ns, "topic")
	entries, err := client.XRange(ctx, streamKey, id, id).Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)

	fields := entries[0].Values
	assert.Equal(t, "1", fields["v"])
	assert.Equal(t, "test-type", fields["type"])
	assert.Equal(t, `{"k":"v"}`, fields["payload"])
	assert.Equal(t, "trace123", fields["trace_id"])
	assert.NotEmpty(t, fields["produced_at"])
	assert.Equal(t, "test-service", fields["producer"])
}

// Test 4: publish_trims_with_approximate_maxlen
func TestProducer_PublishTrimsWithApproximateMaxLen(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Streams.DefaultMaxLen = 10
	cfg = cfg.WithDefaults()

	p := thinrsmq.NewProducer(client, cfg)

	msg := thinrsmq.Message{
		Type:    "test",
		Payload: `{}`,
	}

	// Publish 20 messages
	for i := 0; i < 20; i++ {
		_, err := p.Publish(ctx, "topic", msg)
		require.NoError(t, err)
	}

	streamKey := thinrsmq.StreamKey(ns, "topic")
	length, err := client.XLen(ctx, streamKey).Result()
	require.NoError(t, err)

	// Approximate trimming: should be around 10, but can be up to 2x the target
	// Redis MAXLEN ~ uses node-based trimming which is less precise
	assert.LessOrEqual(t, length, int64(20))
	assert.GreaterOrEqual(t, length, int64(10))
}

// Test 5: publishBatch_returns_correct_number_of_ids
func TestProducer_PublishBatchReturnsCorrectNumberOfIDs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg = cfg.WithDefaults()

	p := thinrsmq.NewProducer(client, cfg)

	msgs := []thinrsmq.Message{
		{Type: "test1", Payload: `{"id":1}`},
		{Type: "test2", Payload: `{"id":2}`},
		{Type: "test3", Payload: `{"id":3}`},
	}

	ids, err := p.PublishBatch(ctx, "topic", msgs)
	require.NoError(t, err)
	require.Len(t, ids, 3)

	// Verify IDs are in ascending order
	assert.Less(t, ids[0], ids[1])
	assert.Less(t, ids[1], ids[2])
}

// Test 6: publish_warns_on_high_stream_length
func TestProducer_PublishWarnsOnHighStreamLength(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Streams.DefaultMaxLen = 100
	cfg = cfg.WithDefaults()

	p := thinrsmq.NewProducer(client, cfg)

	msg := thinrsmq.Message{
		Type:    "test",
		Payload: `{}`,
	}

	// Fill stream close to limit (85 messages = 85% of 100)
	for i := 0; i < 85; i++ {
		_, err := p.Publish(ctx, "topic", msg)
		require.NoError(t, err)
	}

	// Publish one more - should trigger warning
	_, err := p.Publish(ctx, "topic", msg)
	require.NoError(t, err)

	warnings := p.Warnings()
	assert.GreaterOrEqual(t, len(warnings), 1)
	if len(warnings) > 0 {
		assert.Greater(t, warnings[0].CurrentLen, int64(80))
		assert.Equal(t, int64(100), warnings[0].MaxLen)
	}
}

// Test 7: publish_includes_version_field
func TestProducer_PublishIncludesVersionField(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client := NewRedisClient()
	ns := UniqueNamespace(t, client)
	ctx := context.Background()

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg = cfg.WithDefaults()

	p := thinrsmq.NewProducer(client, cfg)

	msg := thinrsmq.Message{
		Type:    "test",
		Payload: `{}`,
	}

	id, err := p.Publish(ctx, "topic", msg)
	require.NoError(t, err)

	streamKey := thinrsmq.StreamKey(ns, "topic")
	entries, err := client.XRange(ctx, streamKey, id, id).Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)

	assert.Equal(t, "1", entries[0].Values["v"])
}
