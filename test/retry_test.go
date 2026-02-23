package test

import (
	"context"
	"github.com/your-org/thin-redis-queue/thinrsmq-go/pkg/thinrsmq"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryStore_SetCreatesHashWithAllFields(t *testing.T) {
	client := NewRedisClient()
	ns := UniqueNamespace(t, client)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	store := thinrsmq.NewRetryStore(client, cfg)

	ctx := context.Background()
	now := time.Now().UTC().UnixMilli()

	err := store.Set(ctx, "topic", "1-0", thinrsmq.RetryInfo{
		Attempt:       1,
		MaxAttempts:   5,
		NextRetryAt:   now + 1000,
		LastError:     "test error",
		FirstFailedAt: now,
	})
	require.NoError(t, err)

	info, err := store.Get(ctx, "topic", "1-0")
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, 1, info.Attempt)
	assert.Equal(t, 5, info.MaxAttempts)
	assert.Equal(t, now+1000, info.NextRetryAt)
	assert.Equal(t, "test error", info.LastError)
	assert.Equal(t, now, info.FirstFailedAt)
}

func TestRetryStore_GetReturnsNilForNonexistentKey(t *testing.T) {
	client := NewRedisClient()
	ns := UniqueNamespace(t, client)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	store := thinrsmq.NewRetryStore(client, cfg)

	ctx := context.Background()
	info, err := store.Get(ctx, "topic", "nonexistent-0")
	require.NoError(t, err)
	assert.Nil(t, info)
}

func TestRetryStore_DeleteRemovesKey(t *testing.T) {
	client := NewRedisClient()
	ns := UniqueNamespace(t, client)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	store := thinrsmq.NewRetryStore(client, cfg)

	ctx := context.Background()
	now := time.Now().UTC().UnixMilli()

	err := store.Set(ctx, "topic", "1-0", thinrsmq.RetryInfo{
		Attempt:       1,
		MaxAttempts:   5,
		NextRetryAt:   now + 1000,
		LastError:     "test error",
		FirstFailedAt: now,
	})
	require.NoError(t, err)

	err = store.Delete(ctx, "topic", "1-0")
	require.NoError(t, err)

	info, err := store.Get(ctx, "topic", "1-0")
	require.NoError(t, err)
	assert.Nil(t, info)
}

func TestRetryStore_InitIfNotExistsOnlyWritesIfAbsent(t *testing.T) {
	client := NewRedisClient()
	ns := UniqueNamespace(t, client)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	store := thinrsmq.NewRetryStore(client, cfg)

	ctx := context.Background()
	now := time.Now().UTC().UnixMilli()

	// Pre-set with attempt=3
	err := store.Set(ctx, "topic", "1-0", thinrsmq.RetryInfo{
		Attempt:       3,
		MaxAttempts:   5,
		NextRetryAt:   now + 5000,
		LastError:     "original error",
		FirstFailedAt: now,
	})
	require.NoError(t, err)

	// Try to init -- should NOT overwrite
	created, err := store.InitIfNotExists(ctx, "topic", "1-0", "new error")
	require.NoError(t, err)
	assert.False(t, created)

	info, err := store.Get(ctx, "topic", "1-0")
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, 3, info.Attempt) // NOT overwritten to 1
	// last_error should be updated per spec section 9.3
	assert.Equal(t, "new error", info.LastError)
}

func TestRetryStore_InitUsesHSETNX(t *testing.T) {
	client := NewRedisClient()
	ns := UniqueNamespace(t, client)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	store := thinrsmq.NewRetryStore(client, cfg)

	ctx := context.Background()

	// First init succeeds
	created, err := store.InitIfNotExists(ctx, "topic", "1-0", "first error")
	require.NoError(t, err)
	assert.True(t, created)

	info, err := store.Get(ctx, "topic", "1-0")
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, 1, info.Attempt)
	assert.Equal(t, "first error", info.LastError)

	// Capture the original nextRetryAt
	originalNextRetryAt := info.NextRetryAt

	// Second init does NOT overwrite attempt/nextRetryAt
	created, err = store.InitIfNotExists(ctx, "topic", "1-0", "different error")
	require.NoError(t, err)
	assert.False(t, created)

	info, err = store.Get(ctx, "topic", "1-0")
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, 1, info.Attempt)                       // unchanged
	assert.Equal(t, originalNextRetryAt, info.NextRetryAt) // unchanged
	// But last_error is updated (per spec section 9.3)
	assert.Equal(t, "different error", info.LastError)
}

func TestRetryStore_HashTTLIs24Hours(t *testing.T) {
	client := NewRedisClient()
	ns := UniqueNamespace(t, client)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	store := thinrsmq.NewRetryStore(client, cfg)

	ctx := context.Background()
	now := time.Now().UTC().UnixMilli()

	err := store.Set(ctx, "topic", "1-0", thinrsmq.RetryInfo{
		Attempt:       1,
		MaxAttempts:   5,
		NextRetryAt:   now + 1000,
		LastError:     "test error",
		FirstFailedAt: now,
	})
	require.NoError(t, err)

	key := thinrsmq.RetryKey(ns, "topic", "1-0")
	ttl, err := client.TTL(ctx, key).Result()
	require.NoError(t, err)

	// TTL should be close to 86400 seconds (24 hours)
	assert.Greater(t, int(ttl.Seconds()), 86000)
	assert.LessOrEqual(t, int(ttl.Seconds()), 86400)

	// Also verify TTL is refreshed after IncrementAttempt
	err = store.IncrementAttempt(ctx, "topic", "1-0", 2, now+5000, "retry fail")
	require.NoError(t, err)

	ttl, err = client.TTL(ctx, key).Result()
	require.NoError(t, err)
	assert.Greater(t, int(ttl.Seconds()), 86000)
	assert.LessOrEqual(t, int(ttl.Seconds()), 86400)
}

func TestRetryStore_IncrementOnlyByMonitor(t *testing.T) {
	client := NewRedisClient()
	ns := UniqueNamespace(t, client)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	store := thinrsmq.NewRetryStore(client, cfg)

	ctx := context.Background()
	now := time.Now().UTC().UnixMilli()

	// Consumer inits at attempt=1
	created, err := store.InitIfNotExists(ctx, "topic", "1-0", "init error")
	require.NoError(t, err)
	assert.True(t, created)

	info, err := store.Get(ctx, "topic", "1-0")
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, 1, info.Attempt)

	// Capture max_attempts and first_failed_at
	originalMaxAttempts := info.MaxAttempts
	originalFirstFailedAt := info.FirstFailedAt

	// Simulate monitor incrementing
	err = store.IncrementAttempt(ctx, "topic", "1-0", 2, now+2000, "retry fail")
	require.NoError(t, err)

	info, err = store.Get(ctx, "topic", "1-0")
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, 2, info.Attempt)
	assert.Equal(t, now+2000, info.NextRetryAt)
	assert.Equal(t, "retry fail", info.LastError)
	// FirstFailedAt and MaxAttempts should be unchanged
	assert.Equal(t, originalFirstFailedAt, info.FirstFailedAt)
	assert.Equal(t, originalMaxAttempts, info.MaxAttempts)
}

func TestRetryStore_SetAppliesTTL(t *testing.T) {
	client := NewRedisClient()
	ns := UniqueNamespace(t, client)

	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = ns
	store := thinrsmq.NewRetryStore(client, cfg)

	ctx := context.Background()
	now := time.Now().UTC().UnixMilli()

	// Verify TTL exists immediately after Set
	err := store.Set(ctx, "topic", "2-0", thinrsmq.RetryInfo{
		Attempt:       1,
		MaxAttempts:   5,
		NextRetryAt:   now + 1000,
		LastError:     "test error",
		FirstFailedAt: now,
	})
	require.NoError(t, err)

	key := thinrsmq.RetryKey(ns, "topic", "2-0")
	ttl, err := client.TTL(ctx, key).Result()
	require.NoError(t, err)

	// TTL should be > 0 and <= 86400 (24 hours)
	assert.Greater(t, int(ttl.Seconds()), 0)
	assert.LessOrEqual(t, int(ttl.Seconds()), 86400)
}
