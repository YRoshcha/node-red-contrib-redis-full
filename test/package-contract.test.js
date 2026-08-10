const assert = require('node:assert/strict');
const fs = require('node:fs');
const Module = require('node:module');
const test = require('node:test');
const { EventEmitter } = require('node:events');

function loadPalette(server) {
  const originalLoad = Module._load;
  const constructed = [];
  const httpRoutes = {};

  class FakeRedis extends EventEmitter {
    constructor(options) {
      super();
      this.options = options;
      constructed.push(options);
    }
    quit() { return Promise.resolve(); }
  }
  FakeRedis.Cluster = class FakeCluster extends FakeRedis {
    constructor(nodes, options) {
      super(options.redisOptions);
      this.nodes = nodes;
    }
  };

  Module._load = function (request, parent, isMain) {
    if (request === 'ioredis') return FakeRedis;
    return originalLoad.call(this, request, parent, isMain);
  };

  const types = new Map();
  const RED = {
    nodes: {
      createNode(node, config) {
        const events = new EventEmitter();
        node.on = events.on.bind(events);
        node.emit = events.emit.bind(events);
        node.credentials = config.credentials || {};
        node.error = () => {};
        node.warn = () => {};
        node.status = () => {};
        node.send = () => {};
      },
      registerType(name, constructor, options) {
        types.set(name, { constructor, options });
      },
      getNode() { return server || null; }
    },
    httpNode: {
      get(path, handler) { httpRoutes[`GET ${path}`] = handler; },
      post(path, handler) { httpRoutes[`POST ${path}`] = handler; }
    },
    settings: { get: () => undefined }
  };

  const modulePath = require.resolve('../redis.js');
  delete require.cache[modulePath];
  require(modulePath)(RED);
  Module._load = originalLoad;
  return { types, constructed, httpRoutes };
}

test('registers all public Redis node types', () => {
  const { types } = loadPalette();
  assert.deepEqual([...types.keys()].sort(), [
    'yroshcha-redis-command', 'yroshcha-redis-config',
    'yroshcha-redis-multi', 'yroshcha-redis-scan',
    'yroshcha-redis-stream-ack', 'yroshcha-redis-stream-api',
    'yroshcha-redis-stream-claim', 'yroshcha-redis-stream-control',
    'yroshcha-redis-stream-gc', 'yroshcha-redis-stream-in',
    'yroshcha-redis-stream-metrics',
    'yroshcha-redis-stream-out', 'yroshcha-redis-subscribe'
  ]);
});

test('package metadata is ready for a public stable release', () => {
  const pkg = JSON.parse(fs.readFileSync(require.resolve('../package.json'), 'utf8'));
  assert.equal(pkg.version, '1.0.0');
  assert.equal(pkg.license, 'MIT');
  assert.match(pkg.repository.url, /^git\+https:\/\/github\.com\/yroshcha\/node-red-contrib-redis-full\.git$/);
  assert.match(pkg.bugs.url, /github\.com\/yroshcha\/node-red-contrib-redis-full\/issues$/);
  assert.equal(pkg.homepage, 'https://github.com/yroshcha/node-red-contrib-redis-full#readme');
  assert.deepEqual(pkg.files, ['redis.js', 'redis.html', 'README.md', 'CHANGELOG.md', 'LICENSE', 'icons']);
  assert.equal(pkg.scripts.prepack, 'npm run check');
  assert.equal(pkg.scripts.prepublishOnly, 'npm run check');
});

test('public README is English-only and documents the stable release', () => {
  const readme = fs.readFileSync(require.resolve('../README.md'), 'utf8');
  assert.doesNotMatch(readme, /[А-Яа-яІіЇїЄєҐґ]/);
  assert.match(readme, /\*\*`1\.0\.0` is the first stable release\.\*\*/);
});

test('config client has finite failure bounds and explicit timeouts', () => {
  const { types, constructed } = loadPalette();
  const ConfigNode = types.get('yroshcha-redis-config').constructor;
  const config = new ConfigNode({
    host: 'redis.internal', port: 6380, db: 0, username: 'service',
    connectTimeout: 2500, commandTimeout: 9000, retryMaxDelay: 7000
  });
  config.getClient();
  assert.equal(constructed.length, 1);
  assert.equal(constructed[0].username, 'service');
  assert.equal(constructed[0].connectTimeout, 2500);
  assert.equal(constructed[0].commandTimeout, 9000);
  assert.equal(constructed[0].maxRetriesPerRequest, 3);
  assert.equal(constructed[0].retryStrategy(99), 7000);
});

test('editor exposes bounded stream-consumer controls', () => {
  const html = fs.readFileSync(require.resolve('../redis.html'), 'utf8');
  for (const id of ['readIntervalMs', 'rateLimitPerSecond', 'batchWindowMs', 'batchIntervalMs', 'startPaused', 'maxPending', 'capacityPollMs', 'capacityCheckIntervalMs', 'autoClaim', 'reclaimIdleMs', 'reclaimIntervalMs', 'reclaimCount', 'maxDeliveries', 'deadLetterStream', 'maxMessages', 'deleteAfterAck']) {
    assert.match(html, new RegExp(`node-input-${id}`));
  }
  assert.match(html, /Auto ACK \(unsafe\)/);
});

test('stream consumer can start paused until a control-plane resume', () => {
  const runtime = fs.readFileSync(require.resolve('../redis.js'), 'utf8');
  const html = fs.readFileSync(require.resolve('../redis.html'), 'utf8');
  assert.match(runtime, /node\.startPaused = !!config\.startPaused/);
  assert.match(runtime, /let paused = node\.startPaused/);
  assert.match(html, /Start paused \(wait for resume\)/);
});

test('stream consumer has an optional unlimited-by-default delivery rate limit', () => {
  const runtime = fs.readFileSync(require.resolve('../redis.js'), 'utf8');
  const html = fs.readFileSync(require.resolve('../redis.html'), 'utf8');
  assert.match(runtime, /node\.rateLimitPerSecond = Math\.max\(0, parseFloat\(config\.rateLimitPerSecond\) \|\| 0\)/);
  assert.match(runtime, /async function waitForDeliverySlot\(\)/);
  assert.match(html, /Rate limit \(msg\/s\)/);
});

test('stream consumer supports count-or-time batching when enabled', () => {
  const runtime = fs.readFileSync(require.resolve('../redis.js'), 'utf8');
  const html = fs.readFileSync(require.resolve('../redis.html'), 'utf8');
  assert.match(runtime, /node\.batchWindowMs = Math\.max\(0, parseInt\(config\.batchWindowMs, 10\) \|\| 0\)/);
  assert.match(runtime, /async function flushBatch\(force\)/);
  assert.match(runtime, /batchBuffer\.length >= node\.count/);
  assert.match(html, /Batch wait \(ms\)/);
});

test('stream consumer can pace batches independently of queue depth', () => {
  const runtime = fs.readFileSync(require.resolve('../redis.js'), 'utf8');
  const html = fs.readFileSync(require.resolve('../redis.html'), 'utf8');
  assert.match(runtime, /node\.batchIntervalMs = Math\.max\(0, parseInt\(config\.batchIntervalMs, 10\) \|\| 0\)/);
  assert.match(runtime, /const waitMs = node\.batchIntervalMs - \(Date\.now\(\) - lastBatchSentAt\)/);
  assert.match(html, /Batch interval \(ms\)/);
});

test('stream consumer can limit XREADGROUP frequency independently of queue depth', () => {
  const runtime = fs.readFileSync(require.resolve('../redis.js'), 'utf8');
  const html = fs.readFileSync(require.resolve('../redis.html'), 'utf8');
  assert.match(runtime, /async function waitForReadInterval\(\)/);
  assert.match(runtime, /if \(!await waitForReadInterval\(\)\) continue;/);
  assert.match(html, /Read interval \(ms\)/);
});

test('palette labels and help text are English-only', () => {
  const html = fs.readFileSync(require.resolve('../redis.html'), 'utf8');
  assert.doesNotMatch(html, /[А-Яа-яІіЇїЄєҐґ]/);
});

test('stream metrics can include the configured DLQ length', async () => {
  const server = {
    getClient() {
      return {
        async call(...args) {
          if (args[0] === 'XINFO') return [['name', 'orders-workers', 'consumers', 2, 'lag', 7, 'entries-read', 12, 'last-delivered-id', '5-0']];
          if (args[0] === 'XLEN') return 3;
          throw new Error(`unexpected call: ${args.join(' ')}`);
        },
        async xpending() { return [4]; }
      };
    }
  };
  const { types } = loadPalette(server);
  const MetricsNode = types.get('yroshcha-redis-stream-metrics').constructor;
  const node = new MetricsNode({ server: 'config', streamKey: 'orders:events', group: 'orders-workers', deadLetterStream: 'orders:events:dlq' });
  const output = await new Promise((resolve, reject) => {
    node.emit('input', {}, resolve, (err) => err ? reject(err) : undefined);
  });
  assert.equal(output.payload.deadLetterStream, 'orders:events:dlq');
  assert.equal(output.payload.deadLetterLength, 3);
});

test('Streams API is a source-only HTTP node and redis cmd keeps its input', () => {
  const html = fs.readFileSync(require.resolve('../redis.html'), 'utf8');
  assert.match(html, /allowMsgCommand:[\s\S]*?inputs: 1,\s*outputs: 1/);
  assert.match(html, /yroshcha-redis-stream-api[\s\S]*?inputs: 0,\s*outputs: 1/);
});

test('ACK executes XDEL when deletion is enabled', async () => {
  const calls = [];
  const server = {
    getClient() {
      return {
        async xack(...args) { calls.push(['xack', ...args]); return 1; },
        async xdel(...args) { calls.push(['xdel', ...args]); return 1; }
      };
    }
  };
  const { types } = loadPalette(server);
  const AckNode = types.get('yroshcha-redis-stream-ack').constructor;
  const node = new AckNode({ server: 'config', streamKey: 'orders:events', group: 'orders-workers', deleteAfterAck: true });
  node.status = () => {};
  await new Promise((resolve, reject) => {
    node.emit('input', { streamId: '1-0' }, () => {}, (err) => err ? reject(err) : resolve());
  });
  assert.deepEqual(calls, [
    ['xack', 'orders:events', 'orders-workers', '1-0'],
    ['xdel', 'orders:events', '1-0']
  ]);
});

test('message-level deletion flag overrides the editor default', async () => {
  const calls = [];
  const server = {
    getClient() {
      return {
        async xack(...args) { calls.push(['xack', ...args]); return 1; },
        async xdel(...args) { calls.push(['xdel', ...args]); return 1; }
      };
    }
  };
  const { types } = loadPalette(server);
  const AckNode = types.get('yroshcha-redis-stream-ack').constructor;
  const node = new AckNode({ server: 'config', streamKey: 'orders:events', group: 'orders-workers', deleteAfterAck: true });
  node.status = () => {};
  await new Promise((resolve, reject) => {
    node.emit('input', { streamId: '1-0', deleteAfterAck: false }, () => {}, (err) => err ? reject(err) : resolve());
  });
  assert.deepEqual(calls, [['xack', 'orders:events', 'orders-workers', '1-0']]);
});

test('ACK uses xreadgroup stream metadata when its own fields are empty', async () => {
  const calls = [];
  const server = {
    getClient() {
      return { async xack(...args) { calls.push(args); return 1; } };
    }
  };
  const { types } = loadPalette(server);
  const AckNode = types.get('yroshcha-redis-stream-ack').constructor;
  const node = new AckNode({ server: 'config' });
  await new Promise((resolve, reject) => {
    node.emit('input', { streamId: '2-0', _streamKey: 'orders:events', _streamGroup: 'orders-workers' }, () => {}, (err) => err ? reject(err) : resolve());
  });
  assert.deepEqual(calls, [['orders:events', 'orders-workers', '2-0']]);
});

test('ACK suppresses an expected closed-connection error during shutdown', async () => {
  const server = {
    closing: true,
    getClient() {
      return { async xack() { throw new Error('Connection is closed.'); } };
    }
  };
  const { types } = loadPalette(server);
  const AckNode = types.get('yroshcha-redis-stream-ack').constructor;
  const node = new AckNode({ server: 'config', streamKey: 'orders:events', group: 'orders-workers' });
  await new Promise((resolve, reject) => {
    node.emit('input', { streamId: '3-0' }, () => {}, (err) => err ? reject(err) : resolve());
  });
});

test('xreadgroup output carries internal ACK routing metadata', () => {
  const runtime = fs.readFileSync(require.resolve('../redis.js'), 'utf8');
  assert.match(runtime, /_streamKey: node\.streamKey/);
  assert.match(runtime, /_streamGroup: node\.group/);
});

test('empty consumer name includes the Node-RED node id', () => {
  const runtime = fs.readFileSync(require.resolve('../redis.js'), 'utf8');
  assert.match(runtime, /HOSTNAME \|\| RED\.settings\.get\('flowfile'\) \|\| 'nr'\}-\$\{node\.id\}/);
});

test('stream consumer has automatic abandoned-message recovery', () => {
  const runtime = fs.readFileSync(require.resolve('../redis.js'), 'utf8');
  assert.match(runtime, /async function reclaimAbandoned\(\)/);
  assert.match(runtime, /\.xautoclaim\(/);
  assert.match(runtime, /if \(!node\.autoClaim \|\| recoveringPending\) return/);
});

test('stream consumer keeps source ports while exposing internal control actions', () => {
  const runtime = fs.readFileSync(require.resolve('../redis.js'), 'utf8');
  const html = fs.readFileSync(require.resolve('../redis.html'), 'utf8');
  assert.match(runtime, /\['pause', 'resume', 'drain', 'status'\]/);
  assert.match(runtime, /pendingForThisConsumer/);
  assert.match(html, /inputs: 0,\s*outputs: 1/);
  assert.match(runtime, /node\.redisStreamControl = control/);
});

test('pause fences entries returned by an in-flight XREADGROUP request', () => {
  const runtime = fs.readFileSync(require.resolve('../redis.js'), 'utf8');
  assert.match(runtime, /if \(paused\) \{\n            recoveringPending = true;\n            continue;/);
  assert.match(runtime, /if \(paused\) \{\n              recoveringPending = true;\n              break;/);
  assert.match(runtime, /if \(paused\) return false;/);
});

test('global stream control coordinates consumers in one runtime', () => {
  const runtime = fs.readFileSync(require.resolve('../redis.js'), 'utf8');
  assert.match(runtime, /const streamConsumers = new Map\(\)/);
  assert.match(runtime, /registerType\('yroshcha-redis-stream-control'/);
  assert.match(runtime, /streamConsumers\.set\(node\.id, node\)/);
  assert.match(runtime, /targetNodeId/);
  assert.match(runtime, /consumer\.id === targetNodeId/);
  assert.match(runtime, /nodeId: node\.id/);
});

test('Streams API protects its routes when token protection is enabled', async () => {
  const { types, httpRoutes } = loadPalette();
  const ApiNode = types.get('yroshcha-redis-stream-api').constructor;
  new ApiNode({ enabled: true, requireToken: true, credentials: { token: 'control-secret' } });
  assert.ok(httpRoutes['GET /redis/streams/status']);
  assert.ok(httpRoutes['POST /redis/streams/control']);

  const response = { code: 0, body: null, status(code) { this.code = code; return this; }, json(body) { this.body = body; } };
  await httpRoutes['GET /redis/streams/status']({ get(header) { return header === 'x-api-key' ? 'control-secret' : ''; } }, response);
  assert.equal(response.code, 0);
  assert.equal(response.body.action, 'status');
  assert.deepEqual(response.body.consumers, []);

  const postResponse = { code: 0, body: null, status(code) { this.code = code; return this; }, json(body) { this.body = body; } };
  await httpRoutes['POST /redis/streams/control']({
    body: '{"action":"pause"}',
    get(header) { return header === 'x-api-key' ? 'control-secret' : ''; }
  }, postResponse);
  assert.equal(postResponse.code, 0);
  assert.equal(postResponse.body.action, 'pause');
  assert.deepEqual(postResponse.body.consumers, []);

  const denied = { code: 0, body: null, status(code) { this.code = code; return this; }, json(body) { this.body = body; } };
  await httpRoutes['GET /redis/streams/status']({ get() { return ''; } }, denied);
  assert.equal(denied.code, 401);
  assert.equal(denied.body.error, 'Unauthorized');
});

test('Streams API accepts unauthenticated private-network access by default after enabling', async () => {
  const { types, httpRoutes } = loadPalette();
  const ApiNode = types.get('yroshcha-redis-stream-api').constructor;
  new ApiNode({ enabled: true });
  const response = { code: 0, body: null, status(code) { this.code = code; return this; }, json(body) { this.body = body; } };
  await httpRoutes['GET /redis/streams/status']({ get() { return ''; } }, response);
  assert.equal(response.code, 0);
  assert.equal(response.body.action, 'status');
});

test('Streams API remains disabled until explicitly enabled', async () => {
  const { types, httpRoutes } = loadPalette();
  const ApiNode = types.get('yroshcha-redis-stream-api').constructor;
  new ApiNode({ credentials: { token: 'control-secret' } });
  const response = { code: 0, body: null, status(code) { this.code = code; return this; }, json(body) { this.body = body; } };
  await httpRoutes['GET /redis/streams/status']({ get() { return 'control-secret'; } }, response);
  assert.equal(response.code, 503);
  assert.equal(response.body.error, 'Redis Streams API is not enabled');
});
