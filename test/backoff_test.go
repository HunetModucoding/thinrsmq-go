package test

import (
	"github.com/your-org/thin-redis-queue/thinrsmq-go/pkg/thinrsmq"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnitBackoff_Attempt1ReturnsBaseDelay(t *testing.T) {
	cfg := thinrsmq.BackoffConfig{
		BaseDelayMs: 1000,
		MaxDelayMs:  60000,
		Jitter:      false,
	}

	delay := thinrsmq.ComputeDelay(1, cfg)
	assert.Equal(t, int64(1000), delay)
}

func TestUnitBackoff_Attempt2ReturnsBaseTimes2(t *testing.T) {
	cfg := thinrsmq.BackoffConfig{
		BaseDelayMs: 1000,
		MaxDelayMs:  60000,
		Jitter:      false,
	}

	delay := thinrsmq.ComputeDelay(2, cfg)
	assert.Equal(t, int64(2000), delay)
}

func TestUnitBackoff_Attempt3ReturnsBaseTimes4(t *testing.T) {
	cfg := thinrsmq.BackoffConfig{
		BaseDelayMs: 1000,
		MaxDelayMs:  60000,
		Jitter:      false,
	}

	delay := thinrsmq.ComputeDelay(3, cfg)
	assert.Equal(t, int64(4000), delay)
}

func TestUnitBackoff_DelayCappedAtMaxDelay(t *testing.T) {
	cfg := thinrsmq.BackoffConfig{
		BaseDelayMs: 1000,
		MaxDelayMs:  60000,
		Jitter:      false,
	}

	delay := thinrsmq.ComputeDelay(20, cfg)
	assert.Equal(t, int64(60000), delay)
}

func TestUnitBackoff_JitterAddsRandomnessWithinRange(t *testing.T) {
	cfg := thinrsmq.BackoffConfig{
		BaseDelayMs: 1000,
		MaxDelayMs:  60000,
		Jitter:      true,
	}

	results := make(map[int64]bool)
	for i := 0; i < 100; i++ {
		delay := thinrsmq.ComputeDelay(1, cfg)
		results[delay] = true

		// delay should be in range [baseDelay, baseDelay + baseDelay)
		// which is [1000, 2000)
		assert.GreaterOrEqual(t, delay, int64(1000), "delay should be >= base_delay")
		assert.Less(t, delay, int64(2000), "delay should be < base_delay + base_delay")
	}

	// With 100 runs, we should have more than 1 unique value (probabilistically certain)
	assert.Greater(t, len(results), 1, "jitter should produce varied results")
}

func TestUnitBackoff_Attempt0OrNegativeReturnsBaseDelay(t *testing.T) {
	cfg := thinrsmq.BackoffConfig{
		BaseDelayMs: 1000,
		MaxDelayMs:  60000,
		Jitter:      false,
	}

	delay0 := thinrsmq.ComputeDelay(0, cfg)
	delayNeg := thinrsmq.ComputeDelay(-1, cfg)

	assert.Equal(t, int64(1000), delay0)
	assert.Equal(t, int64(1000), delayNeg)
}
