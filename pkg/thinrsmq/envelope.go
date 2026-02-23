package thinrsmq

import (
	"fmt"
	"strconv"
	"time"
)

const EnvelopeVersion = "1"

// Message represents a message in the stream.
type Message struct {
	ID         string // Stream entry ID (e.g., "1678886400123-0"), set on read
	Version    string // Envelope version, always "1" for current spec
	Type       string // Event type / message type (required)
	Payload    string // JSON string (required)
	TraceID    string // Distributed tracing ID (optional)
	ProducedAt string // ISO 8601 timestamp (set automatically on publish)
	Producer   string // Producing service identifier (optional)
}

// ToStreamFields converts the message to a flat map suitable for XADD.
// Sets ProducedAt to current time if empty. Sets Version to EnvelopeVersion.
func (m *Message) ToStreamFields() map[string]interface{} {
	fields := make(map[string]interface{})

	// Always set version
	fields["v"] = EnvelopeVersion

	// Required fields
	fields["type"] = m.Type
	fields["payload"] = m.Payload

	// ProducedAt - auto-set if empty
	if m.ProducedAt == "" {
		fields["produced_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	} else {
		fields["produced_at"] = m.ProducedAt
	}

	// Optional fields - only include if non-empty
	if m.TraceID != "" {
		fields["trace_id"] = m.TraceID
	}
	if m.Producer != "" {
		fields["producer"] = m.Producer
	}

	return fields
}

// MessageFromStreamFields parses a Message from Redis stream fields.
//
// Returns ErrMessageTrimmed if fields is nil (trimmed message).
// Returns ErrUnknownVersion if v field is not "1".
// Returns MissingFieldError if a required field (type, payload) is absent.
// Treats missing "v" field as version "1" (backward compat) with a logged warning.
func MessageFromStreamFields(id string, fields map[string]interface{}) (*Message, error) {
	// Check for trimmed message
	if fields == nil {
		return nil, ErrMessageTrimmed
	}

	msg := &Message{
		ID: id,
	}

	// Version field - treat missing as v1 for backward compat
	if v, ok := fields["v"]; ok {
		vStr, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("field 'v' is not a string")
		}
		if vStr != "1" {
			return nil, fmt.Errorf("%w: %s", ErrUnknownVersion, vStr)
		}
		msg.Version = vStr
	} else {
		// Missing version - treat as v1 (backward compat)
		msg.Version = "1"
	}

	// Required field: type
	typeVal, ok := fields["type"]
	if !ok {
		return nil, &MissingFieldError{Field: "type"}
	}
	typeStr, ok := typeVal.(string)
	if !ok {
		return nil, fmt.Errorf("field 'type' is not a string")
	}
	msg.Type = typeStr

	// Required field: payload
	payloadVal, ok := fields["payload"]
	if !ok {
		return nil, &MissingFieldError{Field: "payload"}
	}
	payloadStr, ok := payloadVal.(string)
	if !ok {
		return nil, fmt.Errorf("field 'payload' is not a string")
	}
	msg.Payload = payloadStr

	// Required field: produced_at
	if producedAtVal, ok := fields["produced_at"]; ok {
		producedAtStr, ok := producedAtVal.(string)
		if !ok {
			return nil, fmt.Errorf("field 'produced_at' is not a string")
		}
		msg.ProducedAt = producedAtStr
	}

	// Optional fields
	if traceIDVal, ok := fields["trace_id"]; ok {
		if traceIDStr, ok := traceIDVal.(string); ok {
			msg.TraceID = traceIDStr
		}
	}

	if producerVal, ok := fields["producer"]; ok {
		if producerStr, ok := producerVal.(string); ok {
			msg.Producer = producerStr
		}
	}

	return msg, nil
}

// DLQMessage represents a message in the Dead Letter Queue stream.
type DLQMessage struct {
	ID             string // DLQ stream entry ID, set on read
	OriginalID     string // Original stream entry ID
	OriginalStream string // Source stream key
	Version        string // Copied from original
	Type           string // Copied from original
	Payload        string // Copied from original
	TraceID        string // Copied from original
	ProducedAt     string // Copied from original
	Producer       string // Copied from original
	FailedAt       string // ISO 8601 timestamp when moved to DLQ
	TotalAttempts  int    // Total delivery attempts
	LastError      string // Last error message
	ConsumerGroup  string // Consumer group that failed
	ReplayCount    int    // Number of times replayed from DLQ (starts at 0)
}

// NewDLQMessage creates a DLQMessage from an original Message and failure metadata.
func NewDLQMessage(
	original *Message,
	originalStream string,
	lastError string,
	totalAttempts int,
	consumerGroup string,
) *DLQMessage {
	// Truncate error to 1000 characters
	if len(lastError) > 1000 {
		lastError = lastError[:1000]
	}

	return &DLQMessage{
		OriginalID:     original.ID,
		OriginalStream: originalStream,
		Version:        original.Version,
		Type:           original.Type,
		Payload:        original.Payload,
		TraceID:        original.TraceID,
		ProducedAt:     original.ProducedAt,
		Producer:       original.Producer,
		FailedAt:       time.Now().UTC().Format(time.RFC3339Nano),
		TotalAttempts:  totalAttempts,
		LastError:      lastError,
		ConsumerGroup:  consumerGroup,
		ReplayCount:    0,
	}
}

// ToStreamFields converts the DLQ message to a flat map for XADD.
// All numeric fields are converted to strings (Redis stores everything as strings).
func (d *DLQMessage) ToStreamFields() map[string]interface{} {
	fields := make(map[string]interface{})

	// Identity fields
	fields["original_id"] = d.OriginalID
	fields["original_stream"] = d.OriginalStream

	// Copied from original
	fields["v"] = d.Version
	fields["type"] = d.Type
	fields["payload"] = d.Payload
	fields["produced_at"] = d.ProducedAt

	// Optional fields from original
	if d.TraceID != "" {
		fields["trace_id"] = d.TraceID
	}
	if d.Producer != "" {
		fields["producer"] = d.Producer
	}

	// DLQ-specific fields
	fields["failed_at"] = d.FailedAt
	fields["total_attempts"] = strconv.Itoa(d.TotalAttempts)
	fields["last_error"] = d.LastError
	fields["consumer_group"] = d.ConsumerGroup
	fields["replay_count"] = strconv.Itoa(d.ReplayCount)

	return fields
}

// DLQMessageFromStreamFields parses a DLQMessage from Redis stream fields.
func DLQMessageFromStreamFields(id string, fields map[string]interface{}) (*DLQMessage, error) {
	if fields == nil {
		return nil, ErrMessageTrimmed
	}

	msg := &DLQMessage{
		ID: id,
	}

	// Extract all fields with type checks
	if v, ok := fields["original_id"]; ok {
		msg.OriginalID = v.(string)
	}
	if v, ok := fields["original_stream"]; ok {
		msg.OriginalStream = v.(string)
	}
	if v, ok := fields["v"]; ok {
		msg.Version = v.(string)
	}
	if v, ok := fields["type"]; ok {
		msg.Type = v.(string)
	}
	if v, ok := fields["payload"]; ok {
		msg.Payload = v.(string)
	}
	if v, ok := fields["trace_id"]; ok {
		msg.TraceID = v.(string)
	}
	if v, ok := fields["produced_at"]; ok {
		msg.ProducedAt = v.(string)
	}
	if v, ok := fields["producer"]; ok {
		msg.Producer = v.(string)
	}
	if v, ok := fields["failed_at"]; ok {
		msg.FailedAt = v.(string)
	}
	if v, ok := fields["total_attempts"]; ok {
		attempts, err := strconv.Atoi(v.(string))
		if err == nil {
			msg.TotalAttempts = attempts
		}
	}
	if v, ok := fields["last_error"]; ok {
		msg.LastError = v.(string)
	}
	if v, ok := fields["consumer_group"]; ok {
		msg.ConsumerGroup = v.(string)
	}
	if v, ok := fields["replay_count"]; ok {
		count, err := strconv.Atoi(v.(string))
		if err == nil {
			msg.ReplayCount = count
		}
	}

	return msg, nil
}
