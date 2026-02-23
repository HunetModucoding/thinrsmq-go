package test

import (
	"context"
	"github.com/your-org/thin-redis-queue/thinrsmq-go/pkg/thinrsmq"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestAdmin_PendingStats(t *testing.T) {
	ctx := context.Background()
	client := NewRedisClient()
	ns := UniqueNamespace(t, client)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns

	admin := thinrsmq.NewAdmin(client, cfg)
	producer := thinrsmq.NewProducer(client, cfg)

	topic := "test-topic"
	group := "test-group"
	stream := thinrsmq.StreamKey(ns, topic)

	// Create group
	err := client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	require.NoError(t, err)

	// Publish 3 messages
	for i := 0; i < 3; i++ {
		_, err := producer.Publish(ctx, topic, thinrsmq.Message{
			Type:    "test",
			Payload: `{"i":` + string(rune('0'+i)) + `}`,
		})
		require.NoError(t, err)
	}

	// Read without ACK to create pending entries
	_, err = client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: "test-consumer-1",
		Streams:  []string{stream, ">"},
		Count:    2,
		Block:    -1,
	}).Result()
	require.NoError(t, err)

	// Read one more with different consumer
	_, err = client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: "test-consumer-2",
		Streams:  []string{stream, ">"},
		Count:    1,
		Block:    -1,
	}).Result()
	require.NoError(t, err)

	// Get pending stats
	stats, err := admin.PendingStats(ctx, topic, group)
	require.NoError(t, err)
	require.Equal(t, int64(3), stats.Count)
	require.NotEmpty(t, stats.MinID)
	require.NotEmpty(t, stats.MaxID)
	require.Len(t, stats.Consumers, 2)

	// Verify consumer counts
	consumerMap := make(map[string]int64)
	for _, c := range stats.Consumers {
		consumerMap[c.Name] = c.Count
	}
	require.Equal(t, int64(2), consumerMap["test-consumer-1"])
	require.Equal(t, int64(1), consumerMap["test-consumer-2"])
}

func TestAdmin_ConsumerInfo(t *testing.T) {
	ctx := context.Background()
	client := NewRedisClient()
	ns := UniqueNamespace(t, client)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns

	admin := thinrsmq.NewAdmin(client, cfg)
	producer := thinrsmq.NewProducer(client, cfg)

	topic := "test-topic"
	group := "test-group"
	stream := thinrsmq.StreamKey(ns, topic)

	// Create group
	err := client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	require.NoError(t, err)

	// Publish message
	_, err = producer.Publish(ctx, topic, thinrsmq.Message{
		Type:    "test",
		Payload: `{"test":true}`,
	})
	require.NoError(t, err)

	// Read without ACK
	_, err = client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: "test-consumer-1",
		Streams:  []string{stream, ">"},
		Count:    1,
		Block:    -1,
	}).Result()
	require.NoError(t, err)

	// Wait a bit for idle time
	time.Sleep(100 * time.Millisecond)

	// Get consumer info
	consumers, err := admin.ConsumerInfo(ctx, topic, group)
	require.NoError(t, err)
	require.Len(t, consumers, 1)
	require.Equal(t, "test-consumer-1", consumers[0].Name)
	require.Equal(t, int64(1), consumers[0].Pending)
	require.Greater(t, consumers[0].IdleMs, int64(50)) // Should have some idle time
}

func TestAdmin_StreamInfo(t *testing.T) {
	ctx := context.Background()
	client := NewRedisClient()
	ns := UniqueNamespace(t, client)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns

	admin := thinrsmq.NewAdmin(client, cfg)
	producer := thinrsmq.NewProducer(client, cfg)

	topic := "test-topic"
	stream := thinrsmq.StreamKey(ns, topic)

	// Publish 5 messages
	for i := 0; i < 5; i++ {
		_, err := producer.Publish(ctx, topic, thinrsmq.Message{
			Type:    "test",
			Payload: `{"i":` + string(rune('0'+i)) + `}`,
		})
		require.NoError(t, err)
	}

	// Create 2 groups
	err := client.XGroupCreate(ctx, stream, "group1", "0").Err()
	require.NoError(t, err)
	err = client.XGroupCreate(ctx, stream, "group2", "0").Err()
	require.NoError(t, err)

	// Get stream info
	info, err := admin.StreamInfo(ctx, topic)
	require.NoError(t, err)
	require.Equal(t, int64(5), info.Length)
	require.Equal(t, int64(2), info.Groups)
	require.NotEmpty(t, info.FirstID)
	require.NotEmpty(t, info.LastID)
}
