package thinrsmq

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// PendingInfo holds summary of pending messages for a group.
type PendingInfo struct {
	Count     int64
	MinID     string
	MaxID     string
	Consumers []ConsumerPending
}

// ConsumerPending holds per-consumer pending count.
type ConsumerPending struct {
	Name  string
	Count int64
}

// ConsumerDetail holds info about a consumer in a group.
type ConsumerDetail struct {
	Name    string
	Pending int64
	IdleMs  int64
}

// StreamDetail holds stream metadata.
type StreamDetail struct {
	Length   int64
	Groups   int64
	FirstID  string
	LastID   string
}

// Admin provides monitoring and DLQ management operations.
type Admin struct {
	client *redis.Client
	config Config
	dlq    *DLQ
}

// NewAdmin creates a new Admin instance.
func NewAdmin(client *redis.Client, config Config) *Admin {
	return &Admin{
		client: client,
		config: config,
		dlq:    NewDLQ(client, config),
	}
}

// PendingStats returns pending message summary for a topic+group.
// Uses: XPENDING {stream} {group}
func (a *Admin) PendingStats(ctx context.Context, topic, group string) (*PendingInfo, error) {
	stream := StreamKey(a.config.Namespace, topic)
	pending, err := a.client.XPending(ctx, stream, group).Result()
	if err != nil {
		return nil, err
	}

	consumers := make([]ConsumerPending, 0, len(pending.Consumers))
	for name, count := range pending.Consumers {
		consumers = append(consumers, ConsumerPending{
			Name:  name,
			Count: count,
		})
	}

	minID := ""
	maxID := ""
	if pending.Lower != "" {
		minID = pending.Lower
	}
	if pending.Higher != "" {
		maxID = pending.Higher
	}

	return &PendingInfo{
		Count:     pending.Count,
		MinID:     minID,
		MaxID:     maxID,
		Consumers: consumers,
	}, nil
}

// ConsumerInfo returns details about consumers in a group.
// Uses: XINFO CONSUMERS {stream} {group}
func (a *Admin) ConsumerInfo(ctx context.Context, topic, group string) ([]ConsumerDetail, error) {
	stream := StreamKey(a.config.Namespace, topic)
	consumers, err := a.client.XInfoConsumers(ctx, stream, group).Result()
	if err != nil {
		return nil, err
	}

	details := make([]ConsumerDetail, 0, len(consumers))
	for _, c := range consumers {
		details = append(details, ConsumerDetail{
			Name:    c.Name,
			Pending: c.Pending,
			IdleMs:  c.Idle.Milliseconds(),
		})
	}

	return details, nil
}

// StreamInfo returns stream metadata.
// Uses: XINFO STREAM {stream} and XINFO GROUPS {stream}
func (a *Admin) StreamInfo(ctx context.Context, topic string) (*StreamDetail, error) {
	stream := StreamKey(a.config.Namespace, topic)

	info, err := a.client.XInfoStream(ctx, stream).Result()
	if err != nil {
		return nil, err
	}

	groups, err := a.client.XInfoGroups(ctx, stream).Result()
	if err != nil {
		// If no groups exist, XINFO GROUPS returns an error, but we can continue
		groups = nil
	}

	firstID := ""
	lastID := ""
	if info.FirstEntry.ID != "" {
		firstID = info.FirstEntry.ID
	}
	if info.LastEntry.ID != "" {
		lastID = info.LastEntry.ID
	}

	return &StreamDetail{
		Length:   info.Length,
		Groups:   int64(len(groups)),
		FirstID:  firstID,
		LastID:   lastID,
	}, nil
}

// DLQSize delegates to DLQ.Size.
func (a *Admin) DLQSize(ctx context.Context, topic string) (int64, error) {
	return a.dlq.Size(ctx, topic)
}

// DLQPeek delegates to DLQ.Peek.
func (a *Admin) DLQPeek(ctx context.Context, topic string, count int64) ([]*DLQMessage, error) {
	return a.dlq.Peek(ctx, topic, count)
}

// DLQReplay delegates to DLQ.Replay.
func (a *Admin) DLQReplay(ctx context.Context, topic string, count int64) (int, error) {
	return a.dlq.Replay(ctx, topic, count)
}

// DLQPurge delegates to DLQ.Purge.
func (a *Admin) DLQPurge(ctx context.Context, topic string) (int64, error) {
	return a.dlq.Purge(ctx, topic)
}

// DLQMaxReplays returns the max replays configuration value.
func (a *Admin) DLQMaxReplays() int64 {
	return int64(a.config.DLQ.MaxReplays)
}
