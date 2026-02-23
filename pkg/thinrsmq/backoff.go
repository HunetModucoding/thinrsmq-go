package thinrsmq

import (
	"math"
	"math/rand"
)

// BackoffConfig controls the exponential backoff calculation.
type BackoffConfig struct {
	BaseDelayMs int64 // Base delay in milliseconds
	MaxDelayMs  int64 // Maximum delay cap in milliseconds
	Jitter      bool  // Whether to add random jitter
}

// ComputeDelay calculates the retry delay in milliseconds for the given attempt number.
//
// Formula: min(baseDelay * 2^(attempt-1) + jitter, maxDelay)
// - attempt is 1-indexed (attempt 1 = first retry)
// - jitter is a random value in [0, baseDelay) when enabled
// - attempt <= 0 is treated as attempt 1
func ComputeDelay(attempt int, cfg BackoffConfig) int64 {
	// Normalize attempt: treat 0 or negative as 1
	if attempt <= 0 {
		attempt = 1
	}

	// Calculate exponential part: base * 2^(attempt-1)
	exponent := float64(attempt - 1)
	multiplier := math.Pow(2, exponent)

	// Calculate base delay with exponential backoff
	delay := float64(cfg.BaseDelayMs) * multiplier

	// Add jitter if enabled: random value in [0, baseDelay)
	if cfg.Jitter {
		jitterValue := rand.Float64() * float64(cfg.BaseDelayMs)
		delay += jitterValue
	}

	// Cap at max delay
	if delay > float64(cfg.MaxDelayMs) {
		delay = float64(cfg.MaxDelayMs)
	}

	return int64(delay)
}
