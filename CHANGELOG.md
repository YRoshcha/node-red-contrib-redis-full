# Changelog

All notable changes to this project are documented in this file.

## 1.0.0 — 2026-08-10

First stable public release.

### Added

- Generic Redis command, Pub/Sub, transaction, and cursor-scan nodes.
- Redis Streams publishing, consumer groups, manual ACK, manual recovery, dead-consumer cleanup, and stream metrics.
- Automatic abandoned-entry recovery through `XAUTOCLAIM`, PEL backpressure and alerts, exponential retry backoff with jitter, configurable delivery/batch/read pacing, and optional DLQ handling.
- Local consumer control through flow actions and the optional HTTP control plane: `pause`, `resume`, `status`, and `drain`.
- Controlled consumer startup through **Start paused (wait for resume)**.

### Security and reliability defaults

- HTTP Streams API is disabled by default. When enabled, token protection is optional for private networks and can be required explicitly.
- Automatic ACK is disabled by default; acknowledge only after successful processing.
- Unsafe stream trimming requires explicit confirmation.

### Compatibility

- Node.js 18 or later.
- Node-RED 3.x, 4.x, and 5.x.
- Redis 6.2 or later is recommended for `XAUTOCLAIM`; the remaining functionality supports Redis versions compatible with ioredis 5.

