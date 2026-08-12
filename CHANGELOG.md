# Changelog

All notable changes to this project are documented in this file.

## 1.0.8 — 2026-08-12

### Fixed

- `redis xreadgroup` and `redis xautoclaim` now restore nested object and array fields that were serialised by `redis xadd`, while preserving scalar Redis fields as strings.

## 1.0.7 — 2026-08-12

### Added

- Restored continuous `BLPOP` and `BRPOP` modes in `redis sub`, compatible with the original Redis input node workflow. Configure a list key as Topic and an optional timeout in seconds; the node keeps reading on its dedicated connection.

## 1.0.6 — 2026-08-11

### Fixed

- `redis xreadgroup` now restores its consumer group with `MKSTREAM` when a running consumer receives `NOGROUP`, including a deleted or restarted DLQ stream.

## 1.0.5 — 2026-08-11

### Fixed

- `redis xreadgroup` now raises the dedicated connection command timeout to cover its configured `BLOCK` duration, preventing a valid long poll from being reported as a timeout.
- Stream consumer timing and backoff settings are bounded at runtime even when a malformed flow configuration bypasses editor validation.
- `redis sub` now rejects an empty channel list instead of remaining indefinitely in a starting state.
- `redis xadd` now reports an empty fields object before sending an invalid command to Redis.

### Changed

- `redis scan` now has a 10,000-item per-message safety limit by default. It exposes `msg.scanComplete` and `msg.scanTruncated` so a flow cannot mistake a bounded result for a complete scan.

## 1.0.4 — 2026-08-11

### Fixed

- `redis sub` now retries its initial subscription up to five times with backoff and jitter instead of remaining permanently unsubscribed after one transient timeout.

## 1.0.3 — 2026-08-11

### Fixed

- `redis xreadgroup` now retries dedicated-connection startup and consumer-group initialisation up to five times with backoff and jitter instead of stopping after one transient timeout.

## 1.0.2 — 2026-08-10

Documentation-only patch release.

### Changed

- Removed internal roadmap content from the public README.

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
