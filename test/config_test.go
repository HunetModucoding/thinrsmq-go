package test

import (
	"github.com/your-org/thin-redis-queue/thinrsmq-go/pkg/thinrsmq"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test 4: config validation rejects max_attempts < 1
func TestUnit_Config_ValidationRejectsMaxAttemptsLessThan1(t *testing.T) {
	cfg := thinrsmq.Config{
		Namespace: "test",
		Retry: thinrsmq.RetryConfig{
			MaxAttempts: 0,
		},
	}

	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "max_attempts")
}

// Test 5: config applies defaults for zero values
func TestUnit_Config_AppliesDefaultsForZeroValues(t *testing.T) {
	cfg := thinrsmq.Config{
		Namespace: "test",
	}

	resolved := cfg.WithDefaults()

	// Retry defaults
	assert.Equal(t, 5, resolved.Retry.MaxAttempts)
	assert.Equal(t, int64(1000), resolved.Retry.BaseDelayMs)
	assert.Equal(t, int64(60000), resolved.Retry.MaxDelayMs)
	assert.Equal(t, true, resolved.Retry.Jitter)
	assert.Equal(t, int64(86400), resolved.Retry.HashTTLSeconds)

	// Consumer defaults
	assert.Equal(t, int64(5000), resolved.Consumer.BlockTimeoutMs)
	assert.Equal(t, int64(10), resolved.Consumer.BatchSize)
	assert.Equal(t, int64(30000), resolved.Consumer.ShutdownTimeoutMs) // Review fix #3

	// Streams defaults
	assert.Equal(t, int64(100000), resolved.Streams.DefaultMaxLen)
	assert.Equal(t, int64(10000), resolved.Streams.DLQMaxLen)

	// Monitor defaults
	assert.Equal(t, true, resolved.Monitor.Enabled)
	assert.Equal(t, int64(2000), resolved.Monitor.ScanIntervalMs)
	assert.Equal(t, int64(30000), resolved.Monitor.MinIdleMs)
	assert.Equal(t, int64(100), resolved.Monitor.BatchSize)

	// DLQ defaults
	assert.Equal(t, 3, resolved.DLQ.MaxReplays)

	// Redis defaults
	assert.Equal(t, "localhost:6379", resolved.Redis.Address)
	assert.Equal(t, 10, resolved.Redis.PoolSize)
	assert.Equal(t, int64(3000), resolved.Redis.ReadTimeoutMs)
	assert.Equal(t, int64(3000), resolved.Redis.WriteTimeoutMs)
}

func TestUnit_Config_ValidationRequiresNamespace(t *testing.T) {
	cfg := thinrsmq.Config{}
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "namespace")
}

func TestUnit_Config_ValidationRejectsBatchSizeZero(t *testing.T) {
	cfg := thinrsmq.Config{
		Namespace: "test",
		Retry: thinrsmq.RetryConfig{
			MaxAttempts: 1,
			BaseDelayMs: 1000,
			MaxDelayMs:  1000,
		},
		Streams: thinrsmq.StreamsConfig{
			DefaultMaxLen: 100,
		},
		Consumer: thinrsmq.ConsumerConfig{
			BatchSize: 0,
		},
	}
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "batch_size")
}

func TestUnit_Config_ValidationRejectsBaseDelayZero(t *testing.T) {
	cfg := thinrsmq.Config{
		Namespace: "test",
		Retry: thinrsmq.RetryConfig{
			MaxAttempts: 1,
			BaseDelayMs: 0,
		},
	}
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "base_delay")
}

func TestUnit_Config_ValidationRejectsMaxDelayLessThanBase(t *testing.T) {
	cfg := thinrsmq.Config{
		Namespace: "test",
		Retry: thinrsmq.RetryConfig{
			MaxAttempts: 1,
			BaseDelayMs: 1000,
			MaxDelayMs:  500,
		},
	}
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "max_delay")
}

func TestUnit_Config_ValidationRejectsDefaultMaxLenZero(t *testing.T) {
	cfg := thinrsmq.Config{
		Namespace: "test",
		Retry: thinrsmq.RetryConfig{
			MaxAttempts: 1,
			BaseDelayMs: 1000,
			MaxDelayMs:  1000,
		},
		Consumer: thinrsmq.ConsumerConfig{
			BatchSize: 1,
		},
		Streams: thinrsmq.StreamsConfig{
			DefaultMaxLen: 0,
		},
	}
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "default_max_len")
}
