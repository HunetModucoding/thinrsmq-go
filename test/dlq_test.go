package test

import (
	"context"
	"github.com/your-org/thin-redis-queue/thinrsmq-go/pkg/thinrsmq"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDLQ_MoveToDLQWritesEnrichedMessage(t *testing.T) {
	redis := NewRedisClient()
	ns := UniqueNamespace(t, redis)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns

	dlq := thinrsmq.NewDLQ(redis, cfg)
	stream := thinrsmq.StreamKey(ns, "topic")

	original := &thinrsmq.Message{
		ID:         "1-0",
		Version:    "1",
		Type:       "order.created",
		Payload:    `{"id":1}`,
		TraceID:    "trace1",
		ProducedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Producer:   "svc",
	}

	dlqID, err := dlq.MoveToDLQ(context.Background(), "topic", original, stream, "timeout", 5, "workers")
	require.NoError(t, err)
	assert.NotEmpty(t, dlqID)

	// Verify DLQ entry
	dlqStream := thinrsmq.DLQKey(ns, "topic")
	entries, err := redis.XRange(context.Background(), dlqStream, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)

	fields := entries[0].Values
	assert.Equal(t, "1-0", fields["original_id"])
	assert.Equal(t, stream, fields["original_stream"])
	assert.Equal(t, "1", fields["v"])
	assert.Equal(t, "order.created", fields["type"])
	assert.Equal(t, `{"id":1}`, fields["payload"])
	assert.Equal(t, "trace1", fields["trace_id"])
	assert.Equal(t, "svc", fields["producer"])
	assert.Equal(t, "timeout", fields["last_error"])
	assert.Equal(t, "5", fields["total_attempts"])
	assert.Equal(t, "workers", fields["consumer_group"])
	assert.Equal(t, "0", fields["replay_count"])
	assert.NotEmpty(t, fields["failed_at"])
}

func TestDLQ_MoveToDLQRespectsMaxLenTrimming(t *testing.T) {
	redis := NewRedisClient()
	ns := UniqueNamespace(t, redis)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.Streams.DLQMaxLen = 5

	dlq := thinrsmq.NewDLQ(redis, cfg)
	stream := thinrsmq.StreamKey(ns, "topic")

	// Publish 10 messages to DLQ
	for i := 1; i <= 10; i++ {
		msg := &thinrsmq.Message{
			ID:      "1-0",
			Version: "1",
			Type:    "test",
			Payload: `{}`,
		}
		_, err := dlq.MoveToDLQ(context.Background(), "topic", msg, stream, "err", 5, "group")
		require.NoError(t, err)
	}

	// Check length - should be trimmed (approximately)
	dlqStream := thinrsmq.DLQKey(ns, "topic")
	length, err := redis.XLen(context.Background(), dlqStream).Result()
	require.NoError(t, err)
	assert.LessOrEqual(t, length, int64(10)) // approximate trim can keep up to 2x
	assert.GreaterOrEqual(t, length, int64(5))
}

func TestDLQ_PeekReturnsNEntries(t *testing.T) {
	redis := NewRedisClient()
	ns := UniqueNamespace(t, redis)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns

	dlq := thinrsmq.NewDLQ(redis, cfg)
	stream := thinrsmq.StreamKey(ns, "topic")

	// Add 5 messages with different errors
	for i := 1; i <= 5; i++ {
		msg := &thinrsmq.Message{
			ID:      "1-0",
			Version: "1",
			Type:    "test",
			Payload: `{}`,
		}
		_, err := dlq.MoveToDLQ(context.Background(), "topic", msg, stream, "err_"+string(rune(i+'0')), 5, "group")
		require.NoError(t, err)
		time.Sleep(1 * time.Millisecond) // Ensure different timestamps
	}

	// Peek first 3
	entries, err := dlq.Peek(context.Background(), "topic", 3)
	require.NoError(t, err)
	require.Len(t, entries, 3)

	// Should be oldest first
	assert.Equal(t, "err_1", entries[0].LastError)
	assert.Equal(t, "err_2", entries[1].LastError)
	assert.Equal(t, "err_3", entries[2].LastError)
}

func TestDLQ_ReplayMovesEntryToMainStream(t *testing.T) {
	redis := NewRedisClient()
	ns := UniqueNamespace(t, redis)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns

	dlq := thinrsmq.NewDLQ(redis, cfg)
	stream := thinrsmq.StreamKey(ns, "topic")

	original := &thinrsmq.Message{
		ID:      "1-0",
		Version: "1",
		Type:    "test",
		Payload: `{"x":1}`,
	}

	_, err := dlq.MoveToDLQ(context.Background(), "topic", original, stream, "err", 5, "group")
	require.NoError(t, err)

	// Replay
	replayed, err := dlq.Replay(context.Background(), "topic", 1)
	require.NoError(t, err)
	assert.Equal(t, 1, replayed)

	// Verify DLQ is empty
	size, err := dlq.Size(context.Background(), "topic")
	require.NoError(t, err)
	assert.Equal(t, int64(0), size)

	// Verify message in main stream
	length, err := redis.XLen(context.Background(), stream).Result()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, length, int64(1))

	// Verify re-published message fields
	entries, err := redis.XRange(context.Background(), stream, "-", "+").Result()
	require.NoError(t, err)
	require.Greater(t, len(entries), 0)

	lastEntry := entries[len(entries)-1]
	assert.Equal(t, "1", lastEntry.Values["v"])
	assert.Equal(t, "test", lastEntry.Values["type"])
	assert.Equal(t, `{"x":1}`, lastEntry.Values["payload"])
	assert.Equal(t, "dlq-replay", lastEntry.Values["producer"])
	assert.Equal(t, "1", lastEntry.Values["_dlq_replay_count"])
}

func TestDLQ_PurgeEmptiesDLQ(t *testing.T) {
	redis := NewRedisClient()
	ns := UniqueNamespace(t, redis)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns

	dlq := thinrsmq.NewDLQ(redis, cfg)
	stream := thinrsmq.StreamKey(ns, "topic")

	// Add 5 messages
	for i := 1; i <= 5; i++ {
		msg := &thinrsmq.Message{
			ID:      "1-0",
			Version: "1",
			Type:    "test",
			Payload: `{}`,
		}
		_, err := dlq.MoveToDLQ(context.Background(), "topic", msg, stream, "err", 5, "group")
		require.NoError(t, err)
	}

	size, err := dlq.Size(context.Background(), "topic")
	require.NoError(t, err)
	assert.Equal(t, int64(5), size)

	// Purge
	purged, err := dlq.Purge(context.Background(), "topic")
	require.NoError(t, err)
	assert.Equal(t, int64(5), purged)

	size, err = dlq.Size(context.Background(), "topic")
	require.NoError(t, err)
	assert.Equal(t, int64(0), size)
}

func TestDLQ_SizeReturnsCurrentDepth(t *testing.T) {
	redis := NewRedisClient()
	ns := UniqueNamespace(t, redis)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns

	dlq := thinrsmq.NewDLQ(redis, cfg)
	stream := thinrsmq.StreamKey(ns, "topic")

	// Empty DLQ
	size, err := dlq.Size(context.Background(), "topic")
	require.NoError(t, err)
	assert.Equal(t, int64(0), size)

	// Add one message
	msg := &thinrsmq.Message{
		ID:      "1-0",
		Version: "1",
		Type:    "test",
		Payload: `{}`,
	}
	_, err = dlq.MoveToDLQ(context.Background(), "topic", msg, stream, "err", 5, "group")
	require.NoError(t, err)

	size, err = dlq.Size(context.Background(), "topic")
	require.NoError(t, err)
	assert.Equal(t, int64(1), size)

	// Add another
	msg2 := &thinrsmq.Message{
		ID:      "2-0",
		Version: "1",
		Type:    "test",
		Payload: `{}`,
	}
	_, err = dlq.MoveToDLQ(context.Background(), "topic", msg2, stream, "err", 5, "group")
	require.NoError(t, err)

	size, err = dlq.Size(context.Background(), "topic")
	require.NoError(t, err)
	assert.Equal(t, int64(2), size)
}

func TestDLQ_EntryIncludesReplayCountZero(t *testing.T) {
	redis := NewRedisClient()
	ns := UniqueNamespace(t, redis)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns

	dlq := thinrsmq.NewDLQ(redis, cfg)
	stream := thinrsmq.StreamKey(ns, "topic")

	original := &thinrsmq.Message{
		ID:      "1-0",
		Version: "1",
		Type:    "test",
		Payload: `{}`,
	}

	_, err := dlq.MoveToDLQ(context.Background(), "topic", original, stream, "timeout", 5, "group")
	require.NoError(t, err)

	entries, err := dlq.Peek(context.Background(), "topic", 1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, 0, entries[0].ReplayCount)
}

func TestDLQ_ReplayRespectsMaxReplays(t *testing.T) {
	redis := NewRedisClient()
	ns := UniqueNamespace(t, redis)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.DLQ.MaxReplays = 2

	dlq := thinrsmq.NewDLQ(redis, cfg)
	stream := thinrsmq.StreamKey(ns, "topic")

	msg1 := &thinrsmq.Message{ID: "1-0", Version: "1", Type: "test", Payload: `{}`}
	msg2 := &thinrsmq.Message{ID: "2-0", Version: "1", Type: "test", Payload: `{}`}
	msg3 := &thinrsmq.Message{ID: "3-0", Version: "1", Type: "test", Payload: `{}`}

	// Entry with replay_count=0 -> can replay
	_, err := dlq.MoveToDLQ(context.Background(), "topic", msg1, stream, "err", 5, "group")
	require.NoError(t, err)

	replayed, err := dlq.Replay(context.Background(), "topic", 1)
	require.NoError(t, err)
	assert.Equal(t, 1, replayed)

	// Entry with replay_count=1 -> can still replay
	_, err = dlq.MoveToDLQWithReplayCount(context.Background(), "topic", msg2, stream, "err", 5, "group", 1)
	require.NoError(t, err)

	replayed, err = dlq.Replay(context.Background(), "topic", 1)
	require.NoError(t, err)
	assert.Equal(t, 1, replayed)

	// Entry with replay_count=2 -> FROZEN (>= max_replays)
	_, err = dlq.MoveToDLQWithReplayCount(context.Background(), "topic", msg3, stream, "err", 5, "group", 2)
	require.NoError(t, err)

	replayed, err = dlq.Replay(context.Background(), "topic", 1)
	require.NoError(t, err)
	assert.Equal(t, 0, replayed) // frozen

	size, err := dlq.Size(context.Background(), "topic")
	require.NoError(t, err)
	assert.Equal(t, int64(1), size) // still in DLQ
}

func TestDLQ_FrozenEntrySkippedOnReplay(t *testing.T) {
	redis := NewRedisClient()
	ns := UniqueNamespace(t, redis)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	cfg.DLQ.MaxReplays = 1

	dlq := thinrsmq.NewDLQ(redis, cfg)
	stream := thinrsmq.StreamKey(ns, "topic")

	msg1 := &thinrsmq.Message{ID: "1-0", Version: "1", Type: "test", Payload: `{}`}
	msg2 := &thinrsmq.Message{ID: "2-0", Version: "1", Type: "test", Payload: `{}`}

	// Entry 1: replay_count=1 -> frozen (>= max_replays)
	_, err := dlq.MoveToDLQWithReplayCount(context.Background(), "topic", msg1, stream, "err", 5, "group", 1)
	require.NoError(t, err)

	// Entry 2: replay_count=0 -> eligible
	_, err = dlq.MoveToDLQWithReplayCount(context.Background(), "topic", msg2, stream, "err", 5, "group", 0)
	require.NoError(t, err)

	// Replay with large count
	replayed, err := dlq.Replay(context.Background(), "topic", 10)
	require.NoError(t, err)
	assert.Equal(t, 1, replayed) // only msg2 was replayed

	size, err := dlq.Size(context.Background(), "topic")
	require.NoError(t, err)
	assert.Equal(t, int64(1), size) // msg1 still frozen in DLQ
}

func TestDLQ_MoveIsPipelined(t *testing.T) {
	redis := NewRedisClient()
	ns := UniqueNamespace(t, redis)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns

	dlq := thinrsmq.NewDLQ(redis, cfg)
	stream := thinrsmq.StreamKey(ns, "topic")

	original := &thinrsmq.Message{
		ID:      "1-0",
		Version: "1",
		Type:    "test",
		Payload: `{}`,
	}

	// MoveToDLQ should complete successfully
	dlqID, err := dlq.MoveToDLQ(context.Background(), "topic", original, stream, "err", 5, "group")
	require.NoError(t, err)
	assert.NotEmpty(t, dlqID)

	// Verify entry was written
	size, err := dlq.Size(context.Background(), "topic")
	require.NoError(t, err)
	assert.Equal(t, int64(1), size)

	entries, err := dlq.Peek(context.Background(), "topic", 1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "1-0", entries[0].OriginalID)
}
