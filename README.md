# @yroshcha/node-red-contrib-redis-full

Full Redis support for Node-RED: generic commands, Pub/Sub, scans, transactions, and production-oriented Redis Streams consumer groups.

## Palette identity

All worker nodes are in the **Redis Full** category, use a light node colour, and have the Redis icon. The package uses one shared `yroshcha-redis-config` node for regular commands, Streams, and production safeguards such as backoff/jitter, PEL alerts, dead-consumer cleanup, and DLQ handling.

It follows the Node-RED Redis palette convention: implementation lives in `redis.js` and `redis.html`; non-blocking nodes share one ioredis client while blocking operations receive their own connection.

## Node type isolation

Every node type is prefixed with `yroshcha-redis-*`; the palette category is uniquely named **Redis Full** and the package registration key is `yroshcha-redis`. It can coexist with `node-red-contrib-redis`, `@golfvert/node-red-redis`, and other Redis palettes in the same Node-RED installation.

## Nodes

| Palette label | Type | Redis command(s) | Purpose |
|---|---|---|---|
| `redis cmd` | `yroshcha-redis-command` | any | Runs a generic command through `.call()`. `Block` forces a dedicated connection for commands such as `BLPOP`, `BRPOP`, or `WAIT`. |
| `redis sub` | `yroshcha-redis-subscribe` | `SUBSCRIBE` / `PSUBSCRIBE` | Pub/Sub on a dedicated connection. Supports dynamic subscribe/unsubscribe through `msg.subscribe` and `msg.unsubscribe`. |
| `redis multi` | `yroshcha-redis-multi` | `MULTI` / `EXEC` | Atomic transaction from a command list. |
| `redis scan` | `yroshcha-redis-scan` | `SCAN` / `HSCAN` / `SSCAN` / `ZSCAN` | Cursor-based non-blocking scan. |
| `redis xadd` | `yroshcha-redis-stream-out` | `XADD` | Publishes to a stream. `MAXLEN` requires explicit unsafe confirmation. |
| `redis xreadgroup` | `yroshcha-redis-stream-in` | `XGROUP CREATE MKSTREAM`, `XREADGROUP`, `XAUTOCLAIM` | Consumer-group worker with blocking reads, automatic recovery, backoff/jitter, PEL alerting, DLQ, and backpressure. |
| `redis xack` | `yroshcha-redis-stream-ack` | `XACK` | Acknowledges one or more stream IDs. Uses `_streamKey` / `_streamGroup` from xreadgroup when its own fields are empty. |
| `redis xautoclaim` | `yroshcha-redis-stream-claim` | `XAUTOCLAIM` | Manually recovers abandoned PEL entries; processes the cursor incrementally. |
| `redis consumer gc` | `yroshcha-redis-stream-gc` | `XINFO CONSUMERS`, `XGROUP DELCONSUMER` | Removes idle consumer records that have no pending entries. |
| `redis stream metrics` | `yroshcha-redis-stream-metrics` | `XINFO GROUPS`, `XPENDING`, optional `XLEN` | Returns PEL, lag, consumer count, group cursor, and optional DLQ length. |
| `redis streams control` | `yroshcha-redis-stream-control` | — | Flow node for local `pause`, `resume`, `status`, and `drain` actions. |
| `redis streams API` | `yroshcha-redis-stream-api` | HTTP | Optional HTTP control plane: `GET /redis/streams/status` and `POST /redis/streams/control`. Disabled by default. |

## Installation

```bash
cd ~/.node-red
npm install @yroshcha/node-red-contrib-redis-full
```

For a local archive, use `npm install /path/to/yroshcha-node-red-contrib-redis-full-1.0.0.tgz`.

## Connection model

- `yroshcha-redis-config` keeps one shared ioredis client (`getClient()`) for non-blocking nodes: `redis cmd` (without `Block`), `redis multi`, `redis scan`, `redis xadd`, `redis xack`, `redis xautoclaim`, `redis consumer gc`, and `redis stream metrics`.
- Blocking operations use a dedicated connection (`getDedicatedClient()`): `redis sub`, `redis xreadgroup`, and `redis cmd` when `Block` is enabled.

## Release status

**`1.0.0` is the first stable release.** See [CHANGELOG.md](CHANGELOG.md) for release notes and compatibility information.

## Production profile

The Redis client uses bounded failures: `connectTimeout` (10 s), `commandTimeout` (30 s), exponential reconnect up to `retryMaxDelay` (30 s), and at most three retries for an individual non-blocking request. Tune these values to your SLO rather than leaving commands unbounded.

Safe consumer defaults and important settings:

- **Auto ACK = false.** Acknowledge with `redis xack` only after business processing succeeds. Downstream must be idempotent: Streams provides at-least-once, not exactly-once, delivery.
- **Max pending = 1000** stops new intake once the group PEL reaches that ceiling. Choose it based on acceptable latency and downstream memory capacity.
- **Rate limit (msg/s) = 0** is unlimited. For example, `500` limits average downstream delivery to 500 events per second.
- **Read interval (ms) = 0** reads as fast as Redis responds. `1000` calls `XREADGROUP` no more often than once per second, independent of queue depth.
- **Batch wait (ms) = 0** emits one event at a time. With a positive value, the node emits an array once it has collected `COUNT` events or the wait expires. `msg.payload` and `msg.streamId` become arrays.
- **Batch interval (ms)** sets the minimum delay between emitted batches regardless of queue depth. For one batch of up to 50 entries per second, use `COUNT = 50` and `Batch interval = 1000`.
- **Max deliveries = 5.** After the limit is exceeded, an entry is atomically moved to `<stream>:dlq` and acknowledged. In Redis Cluster, source and DLQ keys must share a hash tag, for example `orders:{eu}` and `orders:{eu}:dlq`.
- **Start paused (wait for resume)** makes a consumer create/verify its group without calling `XREADGROUP`. It takes no work until a control-plane `resume` request arrives; use it for controlled pod startup after readiness checks.
- `redis xautoclaim` limits each manual recovery run through `Run limit` (default 1000), avoiding oversized Node-RED payloads.
- `MAXLEN` in `redis xadd` remains blocked until **Allow unsafe MAXLEN trim** is explicitly confirmed. Trimming can remove a payload still represented in a PEL.

## Production readiness boundaries

The package includes unit/contract checks (`npm test`), but throughput certification must be performed against the target Redis deployment and real flow. For 80–100 million events/day (roughly 0.9–1.2k events/s on average), complete these gates before release:

1. Run a soak test for at least 24 hours with peak load and production-sized payloads.
2. Test Redis failover, network interruption, and Node-RED rolling restarts without losing events or performing unintended duplicate acknowledgements.
3. Alert on PEL, DLQ, delivery count, Redis latency, event-loop lag, and heap usage.
4. Verify downstream idempotency and establish a DLQ replay procedure.

## Multi-pod / HPA consumer naming

`redis xreadgroup` and `redis xautoclaim` accept a **Consumer name**. When empty, the package uses `HOSTNAME-node.id`, which is unique per pod and per consumer node on the same host. Do **not** set the same static consumer name on multiple replicas in one group: pending recovery can otherwise mix unfinished entries between pods.

`process.pid` is not suitable as the fallback because it is commonly `1` in every container. When `HOSTNAME` is unavailable, the flow-file name plus `node.id` is used instead.

Each pod restart creates a new hostname and therefore a new consumer entry. Old consumers with `pending = 0` remain visible in `XINFO CONSUMERS` until `redis consumer gc` removes them.

## xreadgroup safeguards

### Backoff and jitter

After an error, the read loop uses exponential backoff with up to 30% random jitter:

- `initialBackoffMs` (default `500`) is the first delay.
- `maxBackoffMs` (default `30000`) is the cap.
- `backoffMultiplier` (default `2`) is applied after each failure.
- The delay resets after the first successful Redis call.

This avoids a thundering herd when many pods reconnect after a Redis restart or failover.

### PEL alert

An optional periodic `XPENDING` summary check runs on the shared client, so it does not interrupt the blocking read loop:

- `pelAlertThreshold` (default `0`) disables alerting at zero.
- `pelCheckIntervalMs` (default `30000`) is the check interval.
- When the threshold is exceeded, the node writes `node.warn(...)` and displays a yellow editor status until the lag recovers.

### Dead-consumer GC

Run `redis consumer gc` from an input message, typically a scheduled Inject node:

- Reads `XINFO CONSUMERS <stream> <group>`.
- Removes only consumers with `pending = 0` and `idle >= minIdleMs` (default 10 minutes), so active or recently restarted workers with unfinished work are not removed.
- Returns `msg.payload = { removed: [...], kept: [...] }` for logs and alerts.

## Choosing a node

- Read or write a single Redis value/command: `redis cmd`.
- Broadcast delivery without persistence: `redis sub`.
- Atomic command list without WATCH: `redis multi`.
- Scan keys or fields without blocking Redis: `redis scan`.
- Reliable delivery with consumer groups, ACK, and recovery: `redis xadd` → `redis xreadgroup` → `redis xack`. Keep `redis xautoclaim` for manual recovery and `redis consumer gc` for periodic cleanup.

## Example flow

```
[yroshcha-redis-config] ← shared configuration for all nodes below

[inject] -> [redis cmd: HGETALL] -> [function] -> [redis cmd: HSET]

[redis sub: app:notify] -> [function: process event]

[inject 30s] -> [redis xautoclaim] -> [function] -> [redis xack]
[inject 1h]  -> [redis consumer gc] -> [function: log removed]
[redis xreadgroup] -> [function: business logic] -> [redis xack]
                                               \-> (catch) -> no ACK -> recovered by xautoclaim
```

## HTTP control plane

Add one `redis streams API` node and enable **Enable HTTP API**. In a private network, it works without a token by default; enable **Require API token** when protection is needed.

- `GET /redis/streams/status` returns local consumer states and their PEL.
- `POST /redis/streams/control` accepts `pause`, `resume`, `status`, or `drain`; filters are `stream`, `group`, and `nodeId`.
- For graceful scale-down, send `{"action":"drain","drainTimeoutMs":30000}` and terminate the instance only after `drained: true`.
- For controlled pod startup, enable **Start paused (wait for resume)** on the consumer. After readiness probes pass, send `{"action":"resume"}`. The setting is local to each consumer node.

`redis stream metrics` can measure DLQ size too: configure **DLQ stream** and read `msg.payload.deadLetterLength`.

## Deliberately not covered

- Sentinel: the config node supports only the `cluster` option (Redis Cluster through `Redis.Cluster`), not Sentinel-specific configuration.
- WATCH-based optimistic locking: `redis multi` is not a replacement for that pattern.
- Direct `prom-client` metrics: PEL alerts use `node.warn`; use `redis cmd: XPENDING` or `XINFO GROUPS` in a dedicated flow for Prometheus/Grafana collection.

## Next improvements

- Add Node-RED runtime/integration coverage with `node-red-node-test-helper` and testcontainers.
- Add Sentinel support to the config node.
