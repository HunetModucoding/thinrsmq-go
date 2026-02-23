package thinrsmq

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RetryInfo holds retry metadata for a message.
type RetryInfo struct {
	Attempt       int
	MaxAttempts   int
	NextRetryAt   int64  // Unix milliseconds
	LastError     string
	FirstFailedAt int64  // Unix milliseconds
}

// RetryStore manages retry metadata hashes in Redis.
type RetryStore struct {
	client *redis.Client
	config Config
}

// NewRetryStore creates a new RetryStore.
func NewRetryStore(client *redis.Client, config Config) *RetryStore {
	return &RetryStore{
		client: client,
		config: config,
	}
}

// InitIfNotExists atomically creates the retry hash only if it does not exist.
// Uses HSETNX on "attempt" field as the existence check.
// Returns true if the hash was created (first failure), false if it already existed.
//
// Implements the single-writer rule from spec section 2.1:
// - Consumer only writes when hash does NOT exist
// - Once hash exists, only the Monitor writes to it
func (s *RetryStore) InitIfNotExists(
	ctx context.Context,
	topic string,
	messageID string,
	lastError string,
) (bool, error) {
	key := RetryKey(s.config.Namespace, topic, messageID)

	// HSETNX returns 1 if field was set (key didn't exist), 0 if it already existed
	created, err := s.client.HSetNX(ctx, key, "attempt", "1").Result()
	if err != nil {
		return false, fmt.Errorf("failed to init retry hash: %w", err)
	}

	if created {
		// We're the first writer — fill the rest of the fields
		now := time.Now().UTC().UnixMilli()
		delay := ComputeDelay(1, BackoffConfig{
			BaseDelayMs: s.config.Retry.BaseDelayMs,
			MaxDelayMs:  s.config.Retry.MaxDelayMs,
			Jitter:      s.config.Retry.Jitter,
		})

		// Truncate error to 1000 characters
		truncatedError := lastError
		if len(truncatedError) > 1000 {
			truncatedError = truncatedError[:1000]
		}

		pipe := s.client.Pipeline()
		pipe.HSet(ctx, key,
			"max_attempts", strconv.Itoa(s.config.Retry.MaxAttempts),
			"next_retry_at", strconv.FormatInt(now+delay, 10),
			"last_error", truncatedError,
			"first_failed_at", strconv.FormatInt(now, 10),
		)
		pipe.Expire(ctx, key, time.Duration(s.config.Retry.HashTTLSeconds)*time.Second)

		_, err = pipe.Exec(ctx)
		if err != nil {
			return false, fmt.Errorf("failed to set retry hash fields: %w", err)
		}
	} else {
		// Hash already exists — only update last_error (per spec section 9.3)
		truncatedError := lastError
		if len(truncatedError) > 1000 {
			truncatedError = truncatedError[:1000]
		}
		err = s.client.HSet(ctx, key, "last_error", truncatedError).Err()
		if err != nil {
			return false, fmt.Errorf("failed to update last_error: %w", err)
		}
	}

	return created, nil
}

// Delete removes the retry hash for a message.
func (s *RetryStore) Delete(ctx context.Context, topic string, messageID string) error {
	key := RetryKey(s.config.Namespace, topic, messageID)
	return s.client.Del(ctx, key).Err()
}

// Get retrieves retry info. Returns nil if key does not exist.
func (s *RetryStore) Get(ctx context.Context, topic string, messageID string) (*RetryInfo, error) {
	key := RetryKey(s.config.Namespace, topic, messageID)

	fields, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get retry hash: %w", err)
	}

	if len(fields) == 0 {
		return nil, nil
	}

	attempt, _ := strconv.Atoi(fields["attempt"])
	maxAttempts, _ := strconv.Atoi(fields["max_attempts"])
	nextRetryAt, _ := strconv.ParseInt(fields["next_retry_at"], 10, 64)
	firstFailedAt, _ := strconv.ParseInt(fields["first_failed_at"], 10, 64)

	return &RetryInfo{
		Attempt:       attempt,
		MaxAttempts:   maxAttempts,
		NextRetryAt:   nextRetryAt,
		LastError:     fields["last_error"],
		FirstFailedAt: firstFailedAt,
	}, nil
}

// Set creates or overwrites retry info. Sets TTL.
func (s *RetryStore) Set(ctx context.Context, topic string, messageID string, info RetryInfo) error {
	key := RetryKey(s.config.Namespace, topic, messageID)

	truncatedError := info.LastError
	if len(truncatedError) > 1000 {
		truncatedError = truncatedError[:1000]
	}

	pipe := s.client.Pipeline()
	pipe.HSet(ctx, key,
		"attempt", strconv.Itoa(info.Attempt),
		"max_attempts", strconv.Itoa(info.MaxAttempts),
		"next_retry_at", strconv.FormatInt(info.NextRetryAt, 10),
		"last_error", truncatedError,
		"first_failed_at", strconv.FormatInt(info.FirstFailedAt, 10),
	)
	pipe.Expire(ctx, key, time.Duration(s.config.Retry.HashTTLSeconds)*time.Second)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to set retry hash: %w", err)
	}

	return nil
}

// IncrementAttempt updates attempt count, next_retry_at, and last_error.
// Used only by the Monitor. Sets TTL.
func (s *RetryStore) IncrementAttempt(
	ctx context.Context,
	topic string,
	messageID string,
	newAttempt int,
	nextRetryAt int64,
	lastError string,
) error {
	key := RetryKey(s.config.Namespace, topic, messageID)

	truncatedError := lastError
	if len(truncatedError) > 1000 {
		truncatedError = truncatedError[:1000]
	}

	pipe := s.client.Pipeline()
	pipe.HSet(ctx, key,
		"attempt", strconv.Itoa(newAttempt),
		"next_retry_at", strconv.FormatInt(nextRetryAt, 10),
		"last_error", truncatedError,
	)
	pipe.Expire(ctx, key, time.Duration(s.config.Retry.HashTTLSeconds)*time.Second)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to increment retry attempt: %w", err)
	}

	return nil
}
