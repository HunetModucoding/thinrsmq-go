package thinrsmq

// StreamKey returns the main stream key: "{namespace}:{topic}"
func StreamKey(namespace, topic string) string {
	return namespace + ":" + topic
}

// DLQKey returns the DLQ stream key: "{namespace}:{topic}:dlq"
func DLQKey(namespace, topic string) string {
	return namespace + ":" + topic + ":dlq"
}

// RetryKey returns the retry metadata hash key: "{namespace}:{topic}:retries:{messageID}"
func RetryKey(namespace, topic, messageID string) string {
	return namespace + ":" + topic + ":retries:" + messageID
}
