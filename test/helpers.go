package test

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// NewRedisClient creates a Redis client for testing.
// Reads REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_USE_TLS env vars.
// Falls back to REDIS_URL for backward compatibility.
// Defaults to localhost:6379 without password or TLS.
func NewRedisClient() *redis.Client {
	// Priority: Individual env vars > REDIS_URL
	host := os.Getenv("REDIS_HOST")
	port := os.Getenv("REDIS_PORT")
	password := os.Getenv("REDIS_PASSWORD")
	useTLSStr := os.Getenv("REDIS_USE_TLS")

	// Fallback to REDIS_URL parsing
	if host == "" || port == "" {
		addr := os.Getenv("REDIS_URL")
		if addr == "" {
			addr = "localhost:6379"
		}
		parts := strings.Split(addr, ":")
		host = parts[0]
		if len(parts) > 1 {
			port = parts[1]
		} else {
			port = "6379"
		}
	}

	// Default port if not set
	if port == "" {
		port = "6379"
	}

	// Parse TLS flag
	useTLS := useTLSStr == "true" || useTLSStr == "1" || useTLSStr == "yes"

	opts := &redis.Options{
		Addr:     host + ":" + port,
		Password: password,
	}

	// Enable TLS if requested
	if useTLS {
		opts.TLSConfig = &tls.Config{
			ServerName: host, // SNI for Azure Redis Cache
		}
	}

	return redis.NewClient(opts)
}

// UniqueNamespace returns a test-scoped namespace like "test-TestName-1707000000-abc123".
// Registers a cleanup function to delete all keys with this prefix.
func UniqueNamespace(t *testing.T, client *redis.Client) string {
	timestamp := time.Now().Unix()
	shortUUID := uuid.New().String()[:8]
	namespace := fmt.Sprintf("test-%s-%d-%s", t.Name(), timestamp, shortUUID)

	t.Cleanup(func() {
		ctx := context.Background()
		if err := CleanupKeys(ctx, client, namespace); err != nil {
			t.Logf("Cleanup failed for namespace %s: %v", namespace, err)
		}
	})

	return namespace
}

// CleanupKeys deletes all Redis keys matching "{namespace}:*" using SCAN + DEL.
func CleanupKeys(ctx context.Context, client *redis.Client, namespace string) error {
	pattern := namespace + ":*"
	iter := client.Scan(ctx, 0, pattern, 0).Iterator()

	for iter.Next(ctx) {
		key := iter.Val()
		if err := client.Del(ctx, key).Err(); err != nil {
			return fmt.Errorf("failed to delete key %s: %w", key, err)
		}
	}

	return iter.Err()
}

// WaitFor polls condition every 100ms until it returns true or timeout expires.
func WaitFor(t *testing.T, condition func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("WaitFor timed out after %v", timeout)
}

// PendingCount returns the number of pending messages for a stream+group.
func PendingCount(ctx context.Context, client *redis.Client, stream, group string) (int64, error) {
	pending, err := client.XPending(ctx, stream, group).Result()
	if err != nil {
		return 0, err
	}
	return pending.Count, nil
}
