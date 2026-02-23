# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2026-02-11

### Notes

- Go version confirmed NOT affected by the `pendingStats` null crash bug (Node.js v0.1.1 fix)
  - go-redis converts XPENDING null responses into Go zero values (`""`, empty map)
  - Go's `len(nil)` and `for range nil` are safe by design
- Go version confirmed NOT affected by test key leaks (Node.js v0.1.2 fix)
  - `UniqueNamespace(t, client)` already auto-registers `t.Cleanup()` correctly

## [0.1.0] - 2026-02-11

### Added

- Producer with single publish and batch publish operations
- Consumer with consumer groups, XAUTOCLAIM-based recovery, and graceful shutdown
- RetryStore with exponential backoff and configurable max attempts
- ClaimMonitor for idle message detection and automatic retry/DLQ routing
- Dead Letter Queue (DLQ) with peek, replay (with replay guard), and purge operations
- Admin facade for stream and consumer group inspection
- Configuration API with defaults, validation, and environment variable support
- `ConfigFromEnv()` helper for reading Redis connection settings from environment variables
- Key builder functions: `StreamKey()`, `DLQKey()`, `RetryKey()`
- Message envelope serialization with version "1" wire format
- Cross-language wire compatibility with Node.js implementation
- Exponential backoff calculator with jitter support
- CLI tools for producer, consumer, and admin operations (in cmd/ directory)
- Comprehensive error handling with sentinel errors
- Full godoc documentation for all public APIs
- Unit and integration test coverage for all components

### Notes

- Requires Redis >= 7.0 for XAUTOCLAIM support
- Requires Go 1.22 or higher
- Uses go-redis/v9 for Redis client

[0.1.1]: https://github.com/your-org/thin-redis-queue/releases/tag/thinrsmq-go/v0.1.1
[0.1.0]: https://github.com/your-org/thin-redis-queue/releases/tag/thinrsmq-go/v0.1.0
