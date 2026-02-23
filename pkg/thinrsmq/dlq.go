package thinrsmq

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// DLQ provides dead letter queue operations.
type DLQ struct {
	client *redis.Client
	config Config
	logger *slog.Logger
}

// NewDLQ creates a new DLQ instance.
func NewDLQ(client *redis.Client, config Config) *DLQ {
	return &DLQ{
		client: client,
		config: config,
		logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}
}

// MoveToDLQ writes an enriched message to the DLQ stream.
// Sets replay_count to 0.
// Uses XADD with MAXLEN ~ {dlq_max_len}.
func (d *DLQ) MoveToDLQ(
	ctx context.Context,
	topic string,
	original *Message,
	originalStream string,
	lastError string,
	totalAttempts int,
	consumerGroup string,
) (string, error) {
	return d.MoveToDLQWithReplayCount(ctx, topic, original, originalStream, lastError, totalAttempts, consumerGroup, 0)
}

// MoveToDLQWithReplayCount writes a DLQ entry with an explicit replay_count.
// Used when a replayed message fails again and re-enters the DLQ.
func (d *DLQ) MoveToDLQWithReplayCount(
	ctx context.Context,
	topic string,
	original *Message,
	originalStream string,
	lastError string,
	totalAttempts int,
	consumerGroup string,
	replayCount int,
) (string, error) {
	dlqStream := DLQKey(d.config.Namespace, topic)

	// Create DLQ message
	dlqMsg := NewDLQMessage(original, originalStream, lastError, totalAttempts, consumerGroup)
	dlqMsg.ReplayCount = replayCount

	// Write to DLQ stream with MAXLEN trimming
	id, err := d.client.XAdd(ctx, &redis.XAddArgs{
		Stream: dlqStream,
		MaxLen: d.config.Streams.DLQMaxLen,
		Approx: true, // Use approximate trimming for performance
		ID:     "*",
		Values: dlqMsg.ToStreamFields(),
	}).Result()

	return id, err
}

// Peek returns the first N entries from the DLQ stream (oldest first).
func (d *DLQ) Peek(ctx context.Context, topic string, count int64) ([]*DLQMessage, error) {
	dlqStream := DLQKey(d.config.Namespace, topic)

	entries, err := d.client.XRangeN(ctx, dlqStream, "-", "+", count).Result()
	if err != nil {
		return nil, err
	}

	messages := make([]*DLQMessage, 0, len(entries))
	for _, entry := range entries {
		msg, err := DLQMessageFromStreamFields(entry.ID, entry.Values)
		if err != nil {
			d.logger.Warn("failed to parse DLQ message", "id", entry.ID, "error", err)
			continue
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

// Replay re-publishes eligible DLQ entries to the main stream.
// Respects the max_replays guard: entries with replay_count >= max_replays are skipped (frozen).
// Returns the number of messages actually replayed.
func (d *DLQ) Replay(ctx context.Context, topic string, count int64) (int, error) {
	dlqStream := DLQKey(d.config.Namespace, topic)
	mainStream := StreamKey(d.config.Namespace, topic)
	maxReplays := d.config.DLQ.MaxReplays

	// Read DLQ entries
	entries, err := d.client.XRangeN(ctx, dlqStream, "-", "+", count).Result()
	if err != nil {
		return 0, err
	}

	replayed := 0
	for _, entry := range entries {
		dlqMsg, err := DLQMessageFromStreamFields(entry.ID, entry.Values)
		if err != nil {
			d.logger.Warn("failed to parse DLQ message", "id", entry.ID, "error", err)
			continue
		}

		// Check replay guard: skip frozen entries
		if dlqMsg.ReplayCount >= maxReplays {
			d.logger.Warn("frozen DLQ entry - max replays reached",
				"original_id", dlqMsg.OriginalID,
				"replay_count", dlqMsg.ReplayCount,
				"max_replays", maxReplays)
			continue
		}

		// Re-publish to main stream with incremented replay count
		now := time.Now().UTC().Format(time.RFC3339Nano)
		fields := map[string]interface{}{
			"v":                   dlqMsg.Version,
			"type":                dlqMsg.Type,
			"payload":             dlqMsg.Payload,
			"produced_at":         now,
			"producer":            "dlq-replay",
			"_dlq_replay_count":   strconv.Itoa(dlqMsg.ReplayCount + 1),
		}
		if dlqMsg.TraceID != "" {
			fields["trace_id"] = dlqMsg.TraceID
		}

		_, err = d.client.XAdd(ctx, &redis.XAddArgs{
			Stream: mainStream,
			MaxLen: d.config.Streams.DefaultMaxLen,
			Approx: true,
			ID:     "*",
			Values: fields,
		}).Result()
		if err != nil {
			d.logger.Warn("failed to replay message", "original_id", dlqMsg.OriginalID, "error", err)
			continue
		}

		// Remove from DLQ
		err = d.client.XDel(ctx, dlqStream, entry.ID).Err()
		if err != nil {
			d.logger.Warn("failed to delete DLQ entry after replay", "dlq_id", entry.ID, "error", err)
			// Continue anyway - message was replayed successfully
		}

		replayed++
	}

	return replayed, nil
}

// Purge removes all entries from the DLQ stream.
// Returns the number of entries removed (or 0 if stream didn't exist).
func (d *DLQ) Purge(ctx context.Context, topic string) (int64, error) {
	dlqStream := DLQKey(d.config.Namespace, topic)

	// Get current length before deleting
	length, err := d.client.XLen(ctx, dlqStream).Result()
	if err != nil {
		return 0, err
	}

	// Delete the stream
	err = d.client.Del(ctx, dlqStream).Err()
	if err != nil {
		return 0, err
	}

	return length, nil
}

// Size returns the current number of entries in the DLQ stream.
func (d *DLQ) Size(ctx context.Context, topic string) (int64, error) {
	dlqStream := DLQKey(d.config.Namespace, topic)
	return d.client.XLen(ctx, dlqStream).Result()
}
