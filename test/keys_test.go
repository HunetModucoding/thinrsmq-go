package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/your-org/thin-redis-queue/thinrsmq-go/pkg/thinrsmq"
)

// Test 1: StreamKey builds correct key
func TestUnit_StreamKey_BuildsCorrectKey(t *testing.T) {
	result := thinrsmq.StreamKey("myapp", "order.created")
	assert.Equal(t, "myapp:order.created", result)
}

// Test 2: DLQKey appends dlq suffix
func TestUnit_DLQKey_AppendsDlqSuffix(t *testing.T) {
	result := thinrsmq.DLQKey("myapp", "order.created")
	assert.Equal(t, "myapp:order.created:dlq", result)
}

// Test 3: RetryKey includes message id
func TestUnit_RetryKey_IncludesMessageID(t *testing.T) {
	result := thinrsmq.RetryKey("myapp", "order.created", "123-0")
	assert.Equal(t, "myapp:order.created:retries:123-0", result)
}
