package test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test 1: redis_connection_smoke_test
func TestRedisConnectionSmoke(t *testing.T) {
	client := NewRedisClient()
	defer client.Close()

	ctx := context.Background()
	result, err := client.Ping(ctx).Result()

	require.NoError(t, err)
	assert.Equal(t, "PONG", result)
}

// Test 2: smoke_test_redis_version
func TestSmokeRedisVersion(t *testing.T) {
	client := NewRedisClient()
	defer client.Close()

	ctx := context.Background()
	info, err := client.Info(ctx, "server").Result()
	require.NoError(t, err)

	// Parse redis_version field from INFO output
	version := parseRedisVersion(info)
	require.NotEmpty(t, version, "Could not parse Redis version from INFO server")

	// Extract major version
	parts := strings.Split(version, ".")
	require.Greater(t, len(parts), 0, "Invalid version format: %s", version)

	majorVersion, err := strconv.Atoi(parts[0])
	require.NoError(t, err, "Failed to parse major version from: %s", version)

	// Assert Redis >= 7.0
	assert.GreaterOrEqual(t, majorVersion, 7,
		"Redis version %s is too old. This library requires Redis >= 7.0 for XCLAIM, XPENDING IDLE, and XINFO support",
		version)
}

// parseRedisVersion extracts the redis_version field from INFO server output.
// Example line: "redis_version:7.2.4"
func parseRedisVersion(info string) string {
	lines := strings.Split(info, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "redis_version:") {
			return strings.TrimPrefix(line, "redis_version:")
		}
	}
	return ""
}
