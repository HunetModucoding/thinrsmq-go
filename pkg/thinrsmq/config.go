package thinrsmq

import (
	"errors"
	"os"
)

type Config struct {
	Namespace string
	Redis     RedisConfig
	Streams   StreamsConfig
	Consumer  ConsumerConfig
	Retry     RetryConfig
	Monitor   MonitorConfig
	DLQ       DLQConfig
}

type RedisConfig struct {
	Address        string // default: "localhost:6379"
	Password       string
	DB             int
	PoolSize       int   // default: 10
	ReadTimeoutMs  int64 // default: 3000
	WriteTimeoutMs int64 // default: 3000
	UseTLS         bool  // default: false
}

type StreamsConfig struct {
	DefaultMaxLen int64 // default: 100000
	DLQMaxLen     int64 // default: 10000
}

type ConsumerConfig struct {
	BlockTimeoutMs    int64  // default: 5000
	BatchSize         int64  // default: 10
	ConsumerName      string // auto-generated if empty
	ShutdownTimeoutMs int64  // default: 30000 [Review Fix #3]
}

type RetryConfig struct {
	MaxAttempts    int   // default: 5
	BaseDelayMs    int64 // default: 1000
	MaxDelayMs     int64 // default: 60000
	Jitter         bool  // default: true
	HashTTLSeconds int64 // default: 86400
}

type MonitorConfig struct {
	Enabled        bool   // default: true
	ScanIntervalMs int64  // default: 2000
	MinIdleMs      int64  // default: 30000
	BatchSize      int64  // default: 100
	ConsumerName   string // auto-generated if empty
}

type DLQConfig struct {
	MaxReplays int // default: 3
}

// DefaultConfig returns a Config with all default values.
// Namespace is left empty and MUST be set by the caller.
func DefaultConfig() Config {
	return Config{
		Namespace: "",
		Redis: RedisConfig{
			Address:        "localhost:6379",
			PoolSize:       10,
			ReadTimeoutMs:  3000,
			WriteTimeoutMs: 3000,
		},
		Streams: StreamsConfig{
			DefaultMaxLen: 100000,
			DLQMaxLen:     10000,
		},
		Consumer: ConsumerConfig{
			BlockTimeoutMs:    5000,
			BatchSize:         10,
			ShutdownTimeoutMs: 30000,
		},
		Retry: RetryConfig{
			MaxAttempts:    5,
			BaseDelayMs:    1000,
			MaxDelayMs:     60000,
			Jitter:         true,
			HashTTLSeconds: 86400,
		},
		Monitor: MonitorConfig{
			Enabled:        true,
			ScanIntervalMs: 2000,
			MinIdleMs:      30000,
			BatchSize:      100,
		},
		DLQ: DLQConfig{
			MaxReplays: 3,
		},
	}
}

// Validate checks that all required fields are set and values are within valid ranges.
// Returns an error describing the first validation failure.
func (c Config) Validate() error {
	if c.Namespace == "" {
		return errors.New("thinrsmq: namespace must not be empty")
	}

	if c.Retry.MaxAttempts < 1 {
		return errors.New("thinrsmq: retry max_attempts must be >= 1")
	}

	if c.Retry.BaseDelayMs <= 0 {
		return errors.New("thinrsmq: retry base_delay must be > 0")
	}

	if c.Retry.MaxDelayMs < c.Retry.BaseDelayMs {
		return errors.New("thinrsmq: retry max_delay must be >= base_delay")
	}

	if c.Streams.DefaultMaxLen <= 0 {
		return errors.New("thinrsmq: streams default_max_len must be > 0")
	}

	if c.Consumer.BatchSize <= 0 {
		return errors.New("thinrsmq: consumer batch_size must be > 0")
	}

	return nil
}

// WithDefaults returns a new Config with zero-value fields replaced by defaults.
func (c Config) WithDefaults() Config {
	defaults := DefaultConfig()
	result := c

	// Preserve namespace
	if result.Namespace == "" {
		result.Namespace = defaults.Namespace
	}

	// Redis
	if result.Redis.Address == "" {
		result.Redis.Address = defaults.Redis.Address
	}
	if result.Redis.PoolSize == 0 {
		result.Redis.PoolSize = defaults.Redis.PoolSize
	}
	if result.Redis.ReadTimeoutMs == 0 {
		result.Redis.ReadTimeoutMs = defaults.Redis.ReadTimeoutMs
	}
	if result.Redis.WriteTimeoutMs == 0 {
		result.Redis.WriteTimeoutMs = defaults.Redis.WriteTimeoutMs
	}

	// Streams
	if result.Streams.DefaultMaxLen == 0 {
		result.Streams.DefaultMaxLen = defaults.Streams.DefaultMaxLen
	}
	if result.Streams.DLQMaxLen == 0 {
		result.Streams.DLQMaxLen = defaults.Streams.DLQMaxLen
	}

	// Consumer
	if result.Consumer.BlockTimeoutMs == 0 {
		result.Consumer.BlockTimeoutMs = defaults.Consumer.BlockTimeoutMs
	}
	if result.Consumer.BatchSize == 0 {
		result.Consumer.BatchSize = defaults.Consumer.BatchSize
	}
	if result.Consumer.ShutdownTimeoutMs == 0 {
		result.Consumer.ShutdownTimeoutMs = defaults.Consumer.ShutdownTimeoutMs
	}

	// Retry
	if result.Retry.MaxAttempts == 0 {
		result.Retry.MaxAttempts = defaults.Retry.MaxAttempts
	}
	if result.Retry.BaseDelayMs == 0 {
		result.Retry.BaseDelayMs = defaults.Retry.BaseDelayMs
	}
	if result.Retry.MaxDelayMs == 0 {
		result.Retry.MaxDelayMs = defaults.Retry.MaxDelayMs
	}
	// Jitter: only set if explicitly false, because Go zero-value is false
	// But we want default to be true, so we need special handling
	if !result.Retry.Jitter && c.Retry.Jitter == false {
		result.Retry.Jitter = defaults.Retry.Jitter
	}
	if result.Retry.HashTTLSeconds == 0 {
		result.Retry.HashTTLSeconds = defaults.Retry.HashTTLSeconds
	}

	// Monitor
	// Enabled: same issue as Jitter - need special handling
	if !result.Monitor.Enabled && c.Monitor.Enabled == false {
		result.Monitor.Enabled = defaults.Monitor.Enabled
	}
	if result.Monitor.ScanIntervalMs == 0 {
		result.Monitor.ScanIntervalMs = defaults.Monitor.ScanIntervalMs
	}
	if result.Monitor.MinIdleMs == 0 {
		result.Monitor.MinIdleMs = defaults.Monitor.MinIdleMs
	}
	if result.Monitor.BatchSize == 0 {
		result.Monitor.BatchSize = defaults.Monitor.BatchSize
	}

	// DLQ
	if result.DLQ.MaxReplays == 0 {
		result.DLQ.MaxReplays = defaults.DLQ.MaxReplays
	}

	return result
}

// ConfigFromEnv reads Redis connection settings from environment variables
// and returns a Config with those values set. Unset variables use defaults.
//
// Environment variables:
//   - REDIS_HOST: Redis hostname (default: "localhost")
//   - REDIS_PORT: Redis port (default: "6379")
//   - REDIS_PASSWORD: Redis password (default: "")
//   - REDIS_USE_TLS: Enable TLS ("true" or "1") (default: false)
//
// The returned Config has Namespace empty -- callers must set it.
// Call WithDefaults() on the result to fill remaining defaults.
func ConfigFromEnv() Config {
	cfg := DefaultConfig()

	host := os.Getenv("REDIS_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("REDIS_PORT")
	if port == "" {
		port = "6379"
	}
	cfg.Redis.Address = host + ":" + port

	if pw := os.Getenv("REDIS_PASSWORD"); pw != "" {
		cfg.Redis.Password = pw
	}

	tlsEnv := os.Getenv("REDIS_USE_TLS")
	cfg.Redis.UseTLS = (tlsEnv == "true" || tlsEnv == "1")

	return cfg
}
