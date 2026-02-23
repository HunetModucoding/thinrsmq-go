package test

import (
	"errors"
	"github.com/your-org/thin-redis-queue/thinrsmq-go/pkg/thinrsmq"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnit_MessageSerializesToFlatStringMap(t *testing.T) {
	msg := &thinrsmq.Message{
		Type:       "order.created",
		Payload:    `{"id":1}`,
		TraceID:    "abc",
		Producer:   "svc",
		ProducedAt: "2025-01-15T10:30:00.000Z",
	}

	fields := msg.ToStreamFields()

	assert.Equal(t, "1", fields["v"])
	assert.Equal(t, "order.created", fields["type"])
	assert.Equal(t, `{"id":1}`, fields["payload"])
	assert.Equal(t, "abc", fields["trace_id"])
	assert.Equal(t, "svc", fields["producer"])
	assert.NotEmpty(t, fields["produced_at"])
	// Verify produced_at is ISO 8601
	_, err := time.Parse(time.RFC3339, fields["produced_at"].(string))
	assert.NoError(t, err)
}

func TestUnit_MessageDeserializesFromStreamFields(t *testing.T) {
	fields := map[string]interface{}{
		"v":           "1",
		"type":        "order.created",
		"payload":     `{"id":1}`,
		"trace_id":    "abc",
		"produced_at": "2025-01-15T10:30:00.000Z",
	}

	msg, err := thinrsmq.MessageFromStreamFields("123-0", fields)

	require.NoError(t, err)
	assert.Equal(t, "123-0", msg.ID)
	assert.Equal(t, "1", msg.Version)
	assert.Equal(t, "order.created", msg.Type)
	assert.Equal(t, `{"id":1}`, msg.Payload)
	assert.Equal(t, "abc", msg.TraceID)
	assert.Equal(t, "2025-01-15T10:30:00.000Z", msg.ProducedAt)
}

func TestUnit_DeserializationFailsOnMissingRequiredFieldType(t *testing.T) {
	fields := map[string]interface{}{
		"v":       "1",
		"payload": "{}",
	}

	_, err := thinrsmq.MessageFromStreamFields("1-0", fields)

	require.Error(t, err)
	var missingFieldErr *thinrsmq.MissingFieldError
	assert.True(t, errors.As(err, &missingFieldErr))
	assert.Equal(t, "type", missingFieldErr.Field)
}

func TestUnit_DeserializationHandlesNilFields(t *testing.T) {
	_, err := thinrsmq.MessageFromStreamFields("1-0", nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, thinrsmq.ErrMessageTrimmed)
}

func TestUnit_EnvelopeIncludesVersion(t *testing.T) {
	msg := &thinrsmq.Message{
		Type:    "test",
		Payload: "{}",
	}

	fields := msg.ToStreamFields()

	assert.Equal(t, "1", fields["v"])
}

func TestUnit_ConsumerRejectsUnknownVersion(t *testing.T) {
	fields := map[string]interface{}{
		"v":       "99",
		"type":    "test",
		"payload": "{}",
	}

	_, err := thinrsmq.MessageFromStreamFields("1-0", fields)

	require.Error(t, err)
	assert.ErrorIs(t, err, thinrsmq.ErrUnknownVersion)
}

func TestUnit_DeserializationAcceptsVersion1(t *testing.T) {
	fields := map[string]interface{}{
		"v":           "1",
		"type":        "test",
		"payload":     "{}",
		"produced_at": "2025-01-15T10:30:00.000Z",
	}

	msg, err := thinrsmq.MessageFromStreamFields("1-0", fields)

	require.NoError(t, err)
	assert.Equal(t, "1", msg.Version)
}

func TestUnit_DeserializationHandlesMissingVersionGracefully(t *testing.T) {
	fields := map[string]interface{}{
		"type":        "test",
		"payload":     "{}",
		"produced_at": "2025-01-15T10:30:00.000Z",
	}

	msg, err := thinrsmq.MessageFromStreamFields("1-0", fields)

	require.NoError(t, err)
	assert.Equal(t, "1", msg.Version) // treated as v1 for backward compat
}

func TestUnit_DLQMessageIncludesReplayCount(t *testing.T) {
	originalMsg := &thinrsmq.Message{
		ID:         "1-0",
		Version:    "1",
		Type:       "test",
		Payload:    "{}",
		ProducedAt: "2025-01-15T10:30:00.000Z",
	}

	dlqMsg := thinrsmq.NewDLQMessage(originalMsg, "myapp:topic", "timeout", 5, "group")
	fields := dlqMsg.ToStreamFields()

	assert.Equal(t, "0", fields["replay_count"])
	assert.Equal(t, "1-0", fields["original_id"])
	assert.Equal(t, "1", fields["v"]) // preserves version from original
	assert.Equal(t, "timeout", fields["last_error"])
	assert.Equal(t, "5", fields["total_attempts"])
	assert.Equal(t, "group", fields["consumer_group"])
	assert.NotEmpty(t, fields["failed_at"])
	// Verify failed_at is ISO 8601
	_, err := time.Parse(time.RFC3339, fields["failed_at"].(string))
	assert.NoError(t, err)
}

func TestUnit_MessageAutoSetsProducedAtIfEmpty(t *testing.T) {
	msg := &thinrsmq.Message{
		Type:    "test",
		Payload: "{}",
		// ProducedAt is empty
	}

	fields := msg.ToStreamFields()

	producedAt := fields["produced_at"].(string)
	assert.NotEmpty(t, producedAt)
	// Should be recent (within last 5 seconds)
	ts, err := time.Parse(time.RFC3339, producedAt)
	require.NoError(t, err)
	assert.WithinDuration(t, time.Now(), ts, 5*time.Second)
}

func TestUnit_ErrorMessageTruncatedTo1000Chars(t *testing.T) {
	originalMsg := &thinrsmq.Message{
		ID:         "1-0",
		Version:    "1",
		Type:       "test",
		Payload:    "{}",
		ProducedAt: "2025-01-15T10:30:00.000Z",
	}

	longError := strings.Repeat("x", 2000)
	dlqMsg := thinrsmq.NewDLQMessage(originalMsg, "myapp:topic", longError, 5, "group")
	fields := dlqMsg.ToStreamFields()

	lastError := fields["last_error"].(string)
	assert.Equal(t, 1000, len(lastError))
}
