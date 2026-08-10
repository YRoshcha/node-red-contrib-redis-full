const Redis = require('ioredis');
const crypto = require('node:crypto');

module.exports = function (RED) {
  // All stream-in nodes in this Node-RED runtime. Used by the optional global
  // control node to drain/pause/resume every consumer before a scale-down.
  const streamConsumers = new Map();
  // Only one API node should be enabled per runtime. Keeping it in a registry
  // lets the fixed HTTP routes survive regular Node-RED deploys safely.
  const streamControlApis = new Map();

  async function executeStreamControl(command) {
    const action = String(command.action || '').toLowerCase();
    if (!['pause', 'resume', 'drain', 'status'].includes(action)) {
      throw new Error('Control action must be pause, resume, drain or status');
    }
    const targetStream = command.stream;
    const targetGroup = command.group;
    const targetNodeId = command.nodeId;
    const consumers = [...streamConsumers.values()]
      .filter((consumer) => !targetStream || consumer.streamKey === targetStream)
      .filter((consumer) => !targetGroup || consumer.group === targetGroup)
      .filter((consumer) => !targetNodeId || consumer.id === targetNodeId);
    const results = await Promise.all(consumers.map((consumer) => consumer.redisStreamControl({
      action,
      drainTimeoutMs: command.drainTimeoutMs
    })));
    return {
      action,
      target: { stream: targetStream || null, group: targetGroup || null, nodeId: targetNodeId || null },
      consumers: results,
      drained: results.every((result) => result.drained)
    };
  }

  function tokenMatches(expected, supplied) {
    if (!expected || !supplied) return false;
    const expectedBuffer = Buffer.from(expected);
    const suppliedBuffer = Buffer.from(supplied);
    return expectedBuffer.length === suppliedBuffer.length && crypto.timingSafeEqual(expectedBuffer, suppliedBuffer);
  }

  function parseHttpBody(body) {
    if (body && typeof body === 'object' && !Buffer.isBuffer(body)) return body;
    const text = Buffer.isBuffer(body) ? body.toString('utf8') : body;
    if (typeof text !== 'string' || text.trim() === '') return {};
    try {
      const parsed = JSON.parse(text);
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error();
      return parsed;
    } catch (_) {
      throw new Error('POST body must be a JSON object, e.g. {"action":"pause"}');
    }
  }

  function getEnabledControlApi(req, res) {
    const apis = [...streamControlApis.values()];
    if (apis.length === 0) {
      res.status(503).json({ error: 'Redis Streams API is not enabled' });
      return null;
    }
    if (apis.length > 1) {
      res.status(409).json({ error: 'More than one Redis Streams API node is enabled in this runtime' });
      return null;
    }
    const api = apis[0];
    const authorization = req.get('authorization') || '';
    const bearer = authorization.startsWith('Bearer ') ? authorization.slice(7) : '';
    const supplied = req.get('x-api-key') || bearer;
    if (api.requireToken && !tokenMatches(api.token, supplied)) {
      res.status(401).json({ error: 'Unauthorized' });
      return null;
    }
    return api;
  }
  // ---------------------------------------------------------------------
  // yroshcha-redis-config — єдина config-нода. Тримає ОДИН спільний клієнт (лінькво
  // створюваний), яким користуються всі неблокуючі ноди, що на неї
  // посилаються через поле "server". Для блокуючих операцій (BLPOP,
  // SUBSCRIBE, XREADGROUP BLOCK) ноди піднімають окреме з'єднання через
  // getDedicatedClient() — так само як задумано у node-red-contrib-redis
  // (опція "block" = "force use new connection").
  // ---------------------------------------------------------------------
  function RedisConfigNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.closing = false;

    node.host = config.host || '127.0.0.1';
    node.port = parseInt(config.port, 10) || 6379;
    node.db = parseInt(config.db, 10) || 0;
    node.username = (config.username || '').trim();
    node.tls = !!config.tls;
    node.cluster = !!config.cluster;
    node.connectTimeout = Math.max(1000, parseInt(config.connectTimeout, 10) || 10000);
    node.commandTimeout = Math.max(1000, parseInt(config.commandTimeout, 10) || 30000);
    node.retryMaxDelay = Math.max(1000, parseInt(config.retryMaxDelay, 10) || 30000);

    const password = (node.credentials && node.credentials.password) || undefined;

    function connectionOptions(extra) {
      return Object.assign({
        host: node.host,
        port: node.port,
        db: node.db,
        username: node.username || undefined,
        password,
        tls: node.tls ? {} : undefined,
        connectTimeout: node.connectTimeout,
        commandTimeout: node.commandTimeout,
        // Fail a stalled request so the node can report it and the flow can
        // apply its own retry/DLQ policy. Infinite queued commands hide outages.
        maxRetriesPerRequest: 3,
        retryStrategy: (attempt) => Math.min(250 * (2 ** Math.min(attempt, 7)), node.retryMaxDelay)
      }, extra || {});
    }

    function makeClient(extra) {
      const options = connectionOptions(extra);
      const client = node.cluster
        ? new Redis.Cluster([{ host: node.host, port: node.port }], {
            redisOptions: options
          })
        : new Redis(options);
      client.on('error', (err) => node.error(`Redis connection error: ${err.message}`));
      return client;
    }

    let sharedClient = null;

    // Спільний клієнт "на конфіг" — усі звичайні (неблокуючі) ноди,
    // що використовують цю ж config-ноду, діляться одним з'єднанням.
    node.getClient = function () {
      if (!sharedClient) {
        sharedClient = makeClient({ enableAutoPipelining: true });
      }
      return sharedClient;
    };

    // Окреме з'єднання — для BLPOP/BRPOP/SUBSCRIBE/XREADGROUP BLOCK тощо.
    // Викликач відповідає за .quit() при закритті своєї ноди.
    node.getDedicatedClient = function (extra) {
      return makeClient(extra);
    };

    node.on('close', function (done) {
      node.closing = true;
      if (sharedClient) {
        sharedClient.quit().then(() => done()).catch(() => done());
      } else {
        done();
      }
    });
  }

  RED.nodes.registerType('yroshcha-redis-config', RedisConfigNode, {
    credentials: { password: { type: 'password' } }
  });

  function getServer(node, config) {
    const server = RED.nodes.getNode(config.server);
    if (!server) {
      node.error('Redis config (server) is not set');
    }
    return server;
  }

  function parseFlatFields(flat) {
    const obj = {};
    for (let i = 0; i < flat.length; i += 2) {
      obj[flat[i]] = flat[i + 1];
    }
    return obj;
  }

  // ---------------------------------------------------------------------
  // yroshcha-redis-command — БУДЬ-ЯКА команда Redis через ioredis .call().
  // "block" форсує окреме з'єднання (для BLPOP/BRPOP/WAIT тощо).
  // ---------------------------------------------------------------------
  function RedisCommandNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.server = getServer(node, config);
    if (!node.server) return;

    node.command = (config.command || '').trim();
    node.allowMsgCommand = config.allowMsgCommand !== false;
    node.block = !!config.block;

    let dedicatedClient = null;
    function client() {
      if (node.block) {
        if (!dedicatedClient) dedicatedClient = node.server.getDedicatedClient();
        return dedicatedClient;
      }
      return node.server.getClient();
    }

    node.on('input', async function (msg, send, done) {
      send = send || function () { node.send.apply(node, arguments); };
      try {
        const command = (node.allowMsgCommand && msg.command) ? String(msg.command) : node.command;
        if (!command) throw new Error('Redis command is not set (node config or msg.command)');

        const args = msg.args;
        if (args !== undefined && !Array.isArray(args)) {
          throw new Error('msg.args must be an array of arguments, e.g. ["mykey", "myvalue"]');
        }

        // Складені команди (XINFO STREAM, SCRIPT LOAD) розбиваємо на токени.
        const tokens = command.split(/\s+/);
        const result = await client().call(...tokens, ...(args || []));

        msg.payload = result;
        msg.command = command;
        node.status({ fill: 'green', shape: 'dot', text: command.toUpperCase() });
        send(msg);
        done();
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: 'error' });
        done(err);
      }
    });

    node.on('close', async function (done) {
      if (dedicatedClient) {
        try { await dedicatedClient.quit(); } catch (e) { /* ignore */ }
      }
      done();
    });
  }

  RED.nodes.registerType('yroshcha-redis-command', RedisCommandNode);

  // ---------------------------------------------------------------------
  // yroshcha-redis-subscribe — SUBSCRIBE/PSUBSCRIBE, завжди на окремому з'єднанні.
  // ---------------------------------------------------------------------
  function RedisSubscribeNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.server = getServer(node, config);
    if (!node.server) return;

    node.mode = config.mode === 'psubscribe' ? 'psubscribe' : 'subscribe';
    node.channels = (config.channels || '').split(',').map((s) => s.trim()).filter(Boolean);

    const client = node.server.getDedicatedClient();
    let subscribed = [];

    const eventName = node.mode === 'psubscribe' ? 'pmessage' : 'message';
    client.on(eventName, (a, b, c) => {
      const outMsg = node.mode === 'psubscribe'
        ? { pattern: a, topic: b, payload: c }
        : { topic: a, payload: b };
      node.send(outMsg);
    });

    async function doSubscribe(channels) {
      if (!channels.length) return;
      if (node.mode === 'psubscribe') await client.psubscribe(...channels);
      else await client.subscribe(...channels);
      subscribed = [...new Set([...subscribed, ...channels])];
      node.status({ fill: 'green', shape: 'dot', text: `listening (${subscribed.length})` });
    }

    async function doUnsubscribe(channels) {
      if (!channels.length) return;
      if (node.mode === 'psubscribe') await client.punsubscribe(...channels);
      else await client.unsubscribe(...channels);
      subscribed = subscribed.filter((c) => !channels.includes(c));
      node.status({ fill: 'green', shape: 'dot', text: `listening (${subscribed.length})` });
    }

    doSubscribe(node.channels).catch((err) => {
      node.status({ fill: 'red', shape: 'ring', text: 'subscribe failed' });
      node.error(`Redis subscribe failed: ${err.message}`);
    });

    node.on('input', async function (msg, send, done) {
      try {
        if (Array.isArray(msg.subscribe) && msg.subscribe.length) await doSubscribe(msg.subscribe);
        if (Array.isArray(msg.unsubscribe) && msg.unsubscribe.length) await doUnsubscribe(msg.unsubscribe);
        done();
      } catch (err) {
        done(err);
      }
    });

    node.on('close', async function (done) {
      try { await client.quit(); } catch (e) { /* ignore */ }
      node.status({});
      done();
    });
  }

  RED.nodes.registerType('yroshcha-redis-subscribe', RedisSubscribeNode);

  // ---------------------------------------------------------------------
  // yroshcha-redis-multi — атомарна транзакція MULTI/EXEC (на спільному з'єднанні).
  // ---------------------------------------------------------------------
  function RedisMultiNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.server = getServer(node, config);
    if (!node.server) return;

    node.on('input', async function (msg, send, done) {
      send = send || function () { node.send.apply(node, arguments); };
      try {
        const commands = msg.commands;
        if (!Array.isArray(commands) || !commands.length) {
          throw new Error('msg.commands must be a non-empty array, e.g. [["set","k","v"],["incr","c"]]');
        }
        for (const c of commands) {
          if (!Array.isArray(c) || !c.length) {
            throw new Error('Each entry in msg.commands must be an array: [command, ...args]');
          }
        }

        const results = await node.server.getClient().multi(commands).exec();
        const errors = results.filter(([err]) => err);
        if (errors.length) {
          throw new Error(`Transaction failed: ${errors.map(([err]) => err.message).join('; ')}`);
        }

        msg.payload = results.map(([, result]) => result);
        node.status({ fill: 'green', shape: 'dot', text: `exec ${commands.length} cmds` });
        send(msg);
        done();
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: 'error' });
        done(err);
      }
    });
  }

  RED.nodes.registerType('yroshcha-redis-multi', RedisMultiNode);

  // ---------------------------------------------------------------------
  // yroshcha-redis-scan — повний курсорний обхід SCAN/HSCAN/SSCAN/ZSCAN.
  // ---------------------------------------------------------------------
  function RedisScanNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.server = getServer(node, config);
    if (!node.server) return;

    node.scanType = config.scanType || 'SCAN';
    node.defaultCount = parseInt(config.count, 10) || 100;

    node.on('input', async function (msg, send, done) {
      send = send || function () { node.send.apply(node, arguments); };
      try {
        const scanType = (msg.scanType || node.scanType).toUpperCase();
        const match = msg.match;
        const count = msg.count !== undefined ? msg.count : node.defaultCount;
        const client = node.server.getClient();

        let key = null;
        if (scanType !== 'SCAN') {
          key = msg.key;
          if (!key) throw new Error(`${scanType} requires msg.key`);
        }

        const methodName = scanType.toLowerCase();
        if (typeof client[methodName] !== 'function') {
          throw new Error(`Unsupported scanType: ${scanType}`);
        }

        let cursor = '0';
        const collected = [];
        do {
          const callArgs = key ? [key, cursor] : [cursor];
          const opts = [];
          if (match) opts.push('MATCH', match);
          opts.push('COUNT', count);

          const [nextCursor, elements] = await client[methodName](...callArgs, ...opts);
          cursor = nextCursor;

          if (scanType === 'HSCAN' || scanType === 'ZSCAN') {
            for (let i = 0; i < elements.length; i += 2) {
              collected.push({ member: elements[i], value: elements[i + 1] });
            }
          } else {
            collected.push(...elements);
          }
        } while (cursor !== '0');

        msg.payload = collected;
        node.status({ fill: 'green', shape: 'dot', text: `${collected.length} items` });
        send(msg);
        done();
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: 'error' });
        done(err);
      }
    });
  }

  RED.nodes.registerType('yroshcha-redis-scan', RedisScanNode);

  // ---------------------------------------------------------------------
  // yroshcha-redis-stream-out — XADD (спрощений шар Streams, спільне з'єднання).
  // ---------------------------------------------------------------------
  function RedisStreamOutNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.server = getServer(node, config);
    if (!node.server) return;

    node.streamKey = config.streamKey || '';
    node.maxlen = config.maxlen ? parseInt(config.maxlen, 10) : null;
    node.approxTrim = config.approxTrim !== false;
    node.unsafeTrim = !!config.unsafeTrim;

    node.status({ fill: 'green', shape: 'dot', text: 'ready' });

    node.on('input', async function (msg, send, done) {
      send = send || function () { node.send.apply(node, arguments); };
      try {
        const streamKey = msg.stream || node.streamKey;
        if (!streamKey) throw new Error('Stream key is not set (node config or msg.stream)');

        const payload = msg.payload;
        if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
          throw new Error('msg.payload must be a flat object (field -> value) for XADD');
        }

        const fields = [];
        for (const [k, v] of Object.entries(payload)) {
          fields.push(k, typeof v === 'object' ? JSON.stringify(v) : String(v));
        }

        const args = [streamKey];
        const maxlen = msg.maxlen !== undefined ? msg.maxlen : node.maxlen;
        const unsafeTrim = msg.allowUnsafeTrim === true || node.unsafeTrim;
        if (maxlen && !unsafeTrim) {
          throw new Error('MAXLEN trimming is disabled by default because it can remove entries still pending in a consumer group');
        }
        if (maxlen) args.push('MAXLEN', node.approxTrim ? '~' : '=', String(maxlen));
        args.push('*', ...fields);

        const id = await node.server.getClient().xadd(...args);

        msg.streamId = id;
        msg.stream = streamKey;
        node.status({ fill: 'green', shape: 'dot', text: `xadd ${id}` });
        send(msg);
        done();
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: 'error' });
        done(err);
      }
    });
  }

  RED.nodes.registerType('yroshcha-redis-stream-out', RedisStreamOutNode);

  // ---------------------------------------------------------------------
  // yroshcha-redis-stream-in — XREADGROUP consumer, завжди окреме з'єднання (BLOCK).
  // ---------------------------------------------------------------------
  function RedisStreamInNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.server = getServer(node, config);
    if (!node.server) return;

    node.streamKey = config.streamKey || '';
    node.group = config.group || '';
    // У контейнері process.pid майже завжди 1 (PID 1 в PID-namespace) —
    // однаковий для всіх реплік HPA-пулу, бо flows.json деплоїться ідентично
    // на кожен под. HOSTNAME у Kubernetes — унікальне ім'я конкретного поду,
    // тому саме він має бути пріоритетним фолбеком, а pid — лише крайній
    // випадок для середовищ без HOSTNAME (напр. локальний запуск поза k8s).
    node.consumer = config.consumer
      || `${process.env.HOSTNAME || RED.settings.get('flowfile') || 'nr'}-${node.id}`;
    // 100 amortises Redis/network round-trips while Max pending still limits
    // the total work allowed into Node-RED.
    node.count = parseInt(config.count, 10) || 100;
    node.blockMs = parseInt(config.blockMs, 10) || 5000;
    node.readIntervalMs = Math.max(0, parseInt(config.readIntervalMs, 10) || 0);
    node.rateLimitPerSecond = Math.max(0, parseFloat(config.rateLimitPerSecond) || 0);
    node.batchWindowMs = Math.max(0, parseInt(config.batchWindowMs, 10) || 0);
    node.batchIntervalMs = Math.max(0, parseInt(config.batchIntervalMs, 10) || 0);
    if (node.batchIntervalMs && !node.batchWindowMs) node.batchWindowMs = node.batchIntervalMs;
    node.batchingEnabled = node.batchWindowMs > 0 || node.batchIntervalMs > 0;
    node.autoAck = !!config.autoAck;
    // Useful for Kubernetes rollouts: the node becomes ready and creates its
    // consumer group, but it does not reserve work until an explicit resume.
    node.startPaused = !!config.startPaused;
    node.startId = config.startId || '$';
    // A group-wide PEL limit provides a real backpressure boundary even though
    // Node-RED does not expose downstream completion to source nodes.
    node.maxPending = Math.max(node.count, parseInt(config.maxPending, 10) || 1000);
    node.capacityPollMs = Math.max(50, parseInt(config.capacityPollMs, 10) || 500);
    node.capacityCheckIntervalMs = Math.max(50, parseInt(config.capacityCheckIntervalMs, 10) || 250);
    // A message that continually fails business processing must not pin the PEL
    // forever. Disabled only when an operator explicitly sets 0.
    node.maxDeliveries = Math.max(0, parseInt(config.maxDeliveries, 10) || 5);
    node.deadLetterStream = config.deadLetterStream || `${node.streamKey}:dlq`;
    node.autoClaim = config.autoClaim !== false;
    node.reclaimIdleMs = Math.max(1000, parseInt(config.reclaimIdleMs, 10) || 60000);
    node.reclaimIntervalMs = Math.max(1000, parseInt(config.reclaimIntervalMs, 10) || 30000);
    node.reclaimCount = Math.max(1, parseInt(config.reclaimCount, 10) || node.count);
    node.drainCheckIntervalMs = Math.max(50, parseInt(config.drainCheckIntervalMs, 10) || 250);

    // Backoff/jitter при помилках циклу — щоб при масовому падінні Redis
    // (рестарт кластера, failover) весь HPA-пул не долбив reconnect
    // синхронно одним і тим же інтервалом ("thundering herd").
    node.initialBackoffMs = parseInt(config.initialBackoffMs, 10) || 500;
    node.maxBackoffMs = parseInt(config.maxBackoffMs, 10) || 30000;
    node.backoffMultiplier = parseFloat(config.backoffMultiplier) || 2;

    // PEL-alert: періодична неблокуюча перевірка розміру pending list через
    // XPENDING на СПІЛЬНОМУ з'єднанні конфігу (не на blockingClient, щоб не
    // заважати BLOCK-циклу). 0 = вимкнено (за замовчуванням).
    node.pelAlertThreshold = parseInt(config.pelAlertThreshold, 10) || 0;
    node.pelCheckIntervalMs = parseInt(config.pelCheckIntervalMs, 10) || 30000;

    if (!node.streamKey || !node.group) {
      node.error('Stream key and consumer group are required');
      return;
    }

    let stopped = false;
    let paused = node.startPaused;
    let blockingClient = null;
    let recoveringPending = true;
    let currentBackoffMs = node.initialBackoffMs;
    let lastPelCheckAt = 0;
    let lastCapacityCheckAt = 0;
    let lastKnownPending = 0;
    let pelWarningActive = false;
    let lastReclaimAt = 0;
    let reclaimCursor = '0';
    let rateTokens = node.rateLimitPerSecond;
    let rateLastRefillAt = Date.now();
    let batchTimer = null;
    let batchFlushPromise = null;
    let lastBatchSentAt = 0;
    let lastReadAt = 0;
    const batchBuffer = [];

    node.status({ fill: 'yellow', shape: 'ring', text: 'starting' });

    async function ensureGroup(client) {
      try {
        await client.xgroup('CREATE', node.streamKey, node.group, node.startId, 'MKSTREAM');
      } catch (err) {
        if (!String(err.message).includes('BUSYGROUP')) throw err;
      }
    }

    async function waitForReadInterval() {
      if (!node.readIntervalMs || !lastReadAt) return true;
      const waitMs = node.readIntervalMs - (Date.now() - lastReadAt);
      if (waitMs > 0) await new Promise((resolve) => setTimeout(resolve, waitMs));
      return !stopped && !paused;
    }

    async function waitForDeliverySlot() {
      if (!node.rateLimitPerSecond) return true;
      while (!stopped && !paused) {
        const now = Date.now();
        const elapsedMs = now - rateLastRefillAt;
        rateLastRefillAt = now;
        rateTokens = Math.min(
          node.rateLimitPerSecond,
          rateTokens + (elapsedMs * node.rateLimitPerSecond / 1000)
        );
        if (rateTokens >= 1) {
          rateTokens -= 1;
          return true;
        }
        const waitMs = Math.max(1, Math.ceil((1 - rateTokens) * 1000 / node.rateLimitPerSecond));
        await new Promise((resolve) => setTimeout(resolve, waitMs));
      }
      return false;
    }

    async function sendEntries(entries) {
      const batched = node.batchingEnabled;
      const ids = entries.map((entry) => entry.id);
      const msg = {
        payload: batched ? entries.map((entry) => entry.payload) : entries[0].payload,
        streamId: batched ? ids : ids[0],
        stream: node.streamKey,
        group: node.group,
        _streamKey: node.streamKey,
        _streamGroup: node.group,
        consumer: node.consumer
      };
      node.send(msg);
      if (node.autoAck) await node.server.getClient().xack(node.streamKey, node.group, ...ids);
    }

    async function flushBatch(force) {
      if (batchFlushPromise) return batchFlushPromise;
      batchFlushPromise = (async () => {
        if (!batchBuffer.length || (paused && !force)) return false;
        if (!force && node.batchIntervalMs && lastBatchSentAt) {
          const waitMs = node.batchIntervalMs - (Date.now() - lastBatchSentAt);
          if (waitMs > 0) await new Promise((resolve) => setTimeout(resolve, waitMs));
          if (stopped || paused) return false;
        }
        if (batchTimer) {
          clearTimeout(batchTimer);
          batchTimer = null;
        }
        const entries = batchBuffer.splice(0, batchBuffer.length);
        await sendEntries(entries);
        lastBatchSentAt = Date.now();
        return true;
      })();
      try {
        return await batchFlushPromise;
      } finally {
        batchFlushPromise = null;
      }
    }

    function scheduleBatchFlush() {
      if (!node.batchWindowMs || batchTimer || !batchBuffer.length) return;
      batchTimer = setTimeout(() => {
        batchTimer = null;
        flushBatch(false).catch((err) => {
          if (!stopped) {
            node.status({ fill: 'red', shape: 'ring', text: 'batch error' });
            node.error(`Redis stream batch flush error: ${err.message}`);
          }
        });
      }, node.batchWindowMs);
    }

    async function checkPelAlert() {
      if (!node.pelAlertThreshold) return;
      const now = Date.now();
      if (now - lastPelCheckAt < node.pelCheckIntervalMs) return;
      lastPelCheckAt = now;

      try {
        // XPENDING <key> <group> без діапазону повертає лише summary:
        // [totalPending, minId, maxId, [[consumer, count], ...]]
        const summary = await node.server.getClient().xpending(node.streamKey, node.group);
        const totalPending = (summary && summary[0]) || 0;

        if (totalPending > node.pelAlertThreshold) {
          pelWarningActive = true;
          node.warn(
            `PEL "${node.group}"@"${node.streamKey}": ${totalPending} pending ` +
            `(threshold ${node.pelAlertThreshold}) — consumer group is falling behind`
          );
          node.status({ fill: 'yellow', shape: 'dot', text: `listening (PEL ${totalPending} ⚠)` });
        } else if (pelWarningActive) {
          pelWarningActive = false;
          node.status({ fill: 'green', shape: 'dot', text: 'listening' });
        }
      } catch (err) {
        // Перевірка PEL — best-effort, не повинна ронити основний цикл читання.
        node.warn(`PEL-check failed: ${err.message}`);
      }
    }

    async function waitForCapacity() {
      // Auto ACK is intentionally not treated as reliable delivery. It has no
      // PEL to gate on, so surface that fact instead of pretending it is safe.
      if (node.autoAck) return true;
      const now = Date.now();
      if (now - lastCapacityCheckAt >= node.capacityCheckIntervalMs) {
        const summary = await node.server.getClient().xpending(node.streamKey, node.group);
        lastKnownPending = Number(summary && summary[0]) || 0;
        lastCapacityCheckAt = now;
      }
      const pending = lastKnownPending;
      if (pending < node.maxPending) return true;

      node.status({ fill: 'yellow', shape: 'ring', text: `backpressure (PEL ${pending})` });
      await new Promise((resolve) => setTimeout(resolve, node.capacityPollMs));
      return false;
    }

    async function shouldDeadLetter(id, payload) {
      if (!node.maxDeliveries) return false;
      // XPENDING range reply: [id, consumer, idleMs, deliveries]. Querying one
      // ID makes the decision deterministic and avoids an unbounded PEL scan.
      const pending = await node.server.getClient().xpending(
        node.streamKey, node.group, id, id, 1, node.consumer
      );
      const deliveryCount = Number(pending && pending[0] && pending[0][3]) || 0;
      if (deliveryCount <= node.maxDeliveries) return false;

      const dlqFields = [
        'sourceStream', node.streamKey,
        'sourceGroup', node.group,
        'sourceId', id,
        'consumer', node.consumer,
        'deliveries', String(deliveryCount),
        'payload', JSON.stringify(payload)
      ];
      // XADD + XACK must happen atomically. In Redis Cluster the source and
      // DLQ streams therefore need the same hash tag, e.g. orders:{eu}:dlq.
      const result = await node.server.getClient().multi()
        .xadd(node.deadLetterStream, '*', ...dlqFields)
        .xack(node.streamKey, node.group, id)
        .exec();
      if (result.some(([err]) => err)) {
        throw new Error(`DLQ transaction failed for ${id}`);
      }
      node.warn(`Moved stream entry ${id} to DLQ after ${deliveryCount} deliveries`);
      return true;
    }

    async function deliverEntry(id, flat, checkDeliveryCount) {
      // A BLOCK request may complete exactly while a pause command is being
      // handled. Do not leak that already-reserved entry downstream; it stays
      // in this consumer's PEL and will be read with ID 0 after resume.
      if (paused) return false;
      if (!Array.isArray(flat)) {
        throw new Error(`Stream entry ${id} no longer has a payload; do not trim streams below the PEL horizon`);
      }
      const payload = parseFlatFields(flat);
      if (checkDeliveryCount && await shouldDeadLetter(id, payload)) return;
      if (!await waitForDeliverySlot() || paused) return false;

      if (node.batchingEnabled) {
        batchBuffer.push({ id, payload });
        if (batchBuffer.length >= node.count) await flushBatch(false);
        else scheduleBatchFlush();
      } else {
        await sendEntries([{ id, payload }]);
      }
      return true;
    }

    async function reclaimAbandoned() {
      // First drain this consumer's own PEL via XREADGROUP 0. Claiming before
      // that would make the same entries appear twice in one startup cycle.
      if (!node.autoClaim || recoveringPending) return;
      const now = Date.now();
      if (now - lastReclaimAt < node.reclaimIntervalMs) return;
      lastReclaimAt = now;

      const res = await node.server.getClient().xautoclaim(
        node.streamKey, node.group, node.consumer, node.reclaimIdleMs,
        reclaimCursor, 'COUNT', node.reclaimCount
      );
      const [nextCursor, entries] = res;
      reclaimCursor = nextCursor || '0';
      for (const [id, flat] of entries || []) {
        if (paused) break;
        await deliverEntry(id, flat, true);
      }
    }

    async function pendingForThisConsumer() {
      const summary = await node.server.getClient().xpending(node.streamKey, node.group);
      const consumers = (summary && summary[3]) || [];
      const own = consumers.find(([name]) => name === node.consumer);
      return own ? Number(own[1]) || 0 : 0;
    }

    async function control(controlMsg) {
      const action = String(controlMsg.action || (controlMsg.payload && controlMsg.payload.action) || '').toLowerCase();
      if (!['pause', 'resume', 'drain', 'status'].includes(action)) {
        throw new Error('Control msg.action must be pause, resume, drain or status');
      }

      if (action === 'pause' || action === 'drain') {
        paused = true;
        // An in-flight XREADGROUP can have reserved entries after startup
        // recovery has finished. Resume must read this consumer's PEL first.
        recoveringPending = true;
      }
      if (action === 'drain') await flushBatch(true);
      if (action === 'resume') {
        paused = false;
        await flushBatch(false);
      }

      let pending = await pendingForThisConsumer();
      let drained = pending === 0;
      if (action === 'drain' && !drained) {
        const timeoutMs = Math.max(1000, parseInt(controlMsg.drainTimeoutMs, 10) || 30000);
        const deadline = Date.now() + timeoutMs;
        while (!stopped && pending > 0 && Date.now() < deadline) {
          await new Promise((resolve) => setTimeout(resolve, node.drainCheckIntervalMs));
          pending = await pendingForThisConsumer();
        }
        drained = pending === 0;
      }

      node.status({
        fill: paused ? 'yellow' : 'green',
        shape: paused ? 'ring' : 'dot',
        text: paused ? (drained ? 'paused (drained)' : `paused (PEL ${pending})`) : 'listening'
      });
      return { nodeId: node.id, stream: node.streamKey, group: node.group, action, paused, drained, pending, consumer: node.consumer };
    }

    node.redisStreamControl = control;
    streamConsumers.set(node.id, node);

    async function loop() {
      blockingClient = node.server.getDedicatedClient({
        // XREADGROUP BLOCK must not stay zombie after a silent network split.
        blockingTimeout: node.blockMs + node.server.commandTimeout + 1000,
        maxRetriesPerRequest: 1,
        autoResendUnfulfilledCommands: false
      });
      blockingClient.on('error', (err) => {
        node.status({ fill: 'red', shape: 'ring', text: 'redis error' });
        node.error(`Redis stream-in error: ${err.message}`);
      });

      await ensureGroup(blockingClient);
      node.status({
        fill: paused ? 'yellow' : 'green',
        shape: paused ? 'ring' : 'dot',
        text: paused ? 'paused (startup)' : 'listening'
      });

      while (!stopped) {
        try {
          if (paused) {
            await new Promise((resolve) => setTimeout(resolve, node.drainCheckIntervalMs));
            continue;
          }
          await checkPelAlert();
          await reclaimAbandoned();
          // Pending recovery is deliberately allowed through even when the PEL
          // is full; otherwise an already-full PEL could never drain.
          if (!recoveringPending && !await waitForCapacity()) continue;
          if (!await waitForReadInterval()) continue;

          const readId = recoveringPending ? '0' : '>';
          lastReadAt = Date.now();
          const res = await blockingClient.xreadgroup(
            'GROUP', node.group, node.consumer,
            'COUNT', node.count,
            'BLOCK', node.blockMs,
            'STREAMS', node.streamKey, readId
          );

          // Успішний виклик (навіть без нових даних) — скидаємо backoff.
          currentBackoffMs = node.initialBackoffMs;

          if (!res) {
            if (recoveringPending) recoveringPending = false;
            continue;
          }

          const [, entries] = res[0];
          if (recoveringPending && entries.length === 0) {
            recoveringPending = false;
            continue;
          }

          // `pause` can arrive while the blocking XREADGROUP is awaiting
          // Redis. The reply is already in this consumer's PEL, but it must
          // not enter business logic after the pause acknowledgement.
          if (paused) {
            recoveringPending = true;
            continue;
          }

          for (const [id, flat] of entries) {
            if (paused) {
              recoveringPending = true;
              break;
            }
            // New `>` entries have one delivery. Pending recovery can cross the
            // DLQ threshold, therefore only it needs the XPENDING check.
            await deliverEntry(id, flat, recoveringPending);
          }
        } catch (err) {
          if (stopped) break;

          const jitter = Math.random() * currentBackoffMs * 0.3; // до +30%
          const delay = Math.min(currentBackoffMs, node.maxBackoffMs) + jitter;

          node.status({ fill: 'red', shape: 'ring', text: `retry in ${Math.round(delay)}ms` });
          node.error(`Redis stream-in loop error: ${err.message}`);
          await new Promise((r) => setTimeout(r, delay));

          currentBackoffMs = Math.min(currentBackoffMs * node.backoffMultiplier, node.maxBackoffMs);
        }
      }
    }

    loop().catch((err) => {
      node.status({ fill: 'red', shape: 'ring', text: 'failed to start' });
      node.error(`Redis stream-in failed to start: ${err.message}`);
    });

    node.on('close', async function (done) {
      stopped = true;
      streamConsumers.delete(node.id);
      if (batchTimer) clearTimeout(batchTimer);
      if (blockingClient) {
        try { blockingClient.disconnect(false); } catch (e) { /* ignore */ }
      }
      node.status({});
      done();
    });
  }

  RED.nodes.registerType('yroshcha-redis-stream-in', RedisStreamInNode);

  // ---------------------------------------------------------------------
  // yroshcha-redis-stream-ack — XACK (спільне з'єднання).
  // ---------------------------------------------------------------------
  function RedisStreamAckNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.server = getServer(node, config);
    if (!node.server) return;

    node.streamKey = config.streamKey || '';
    node.group = config.group || '';
    node.deleteAfterAck = !!config.deleteAfterAck;
    let closing = false;

    node.on('input', async function (msg, send, done) {
      send = send || function () { node.send.apply(node, arguments); };
      try {
        const streamKey = msg.stream || msg._streamKey || node.streamKey;
        const group = msg.group || msg._streamGroup || node.group;
        if (!streamKey || !group) throw new Error('Stream key and group are required');

        const ids = Array.isArray(msg.streamId) ? msg.streamId : [msg.streamId];
        if (!ids.length || !ids[0]) throw new Error('msg.streamId is required');

        const acked = await node.server.getClient().xack(streamKey, group, ...ids);
        msg.acked = acked;
        // A flow can choose retention per event without changing the node
        // configuration: msg.deleteAfterAck overrides the editor default.
        const deleteAfterAck = msg.deleteAfterAck === undefined
          ? node.deleteAfterAck
          : msg.deleteAfterAck === true;
        if (deleteAfterAck && acked > 0) {
          // XDEL is intentionally after XACK: a failed deletion leaves an
          // auditable entry behind instead of risking an unacknowledged loss.
          msg.deleted = await node.server.getClient().xdel(streamKey, ...ids);
        }
        node.status({
          fill: 'green',
          shape: 'dot',
          text: deleteAfterAck ? `acked ${acked}, deleted ${msg.deleted}` : `acked ${acked}`
        });
        send(msg);
        done();
      } catch (err) {
        // Node-RED may close the shared config connection before this node's
        // final ACK finishes during a redeploy/restart. Leave the entry in PEL
        // for recovery, without reporting an expected shutdown race as an error.
        if (closing || node.server.closing) {
          node.status({});
          done();
          return;
        }
        node.status({ fill: 'red', shape: 'ring', text: 'error' });
        done(err);
      }
    });

    node.on('close', function (done) {
      closing = true;
      done();
    });
  }

  RED.nodes.registerType('yroshcha-redis-stream-ack', RedisStreamAckNode);

  // ---------------------------------------------------------------------
  // yroshcha-redis-stream-claim — XAUTOCLAIM, повний прохід по курсору за виклик.
  // ---------------------------------------------------------------------
  function RedisStreamClaimNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.server = getServer(node, config);
    if (!node.server) return;

    node.streamKey = config.streamKey || '';
    node.group = config.group || '';
    node.consumer = config.consumer || '';
    node.minIdleMs = parseInt(config.minIdleMs, 10) || 60000;
    node.count = parseInt(config.count, 10) || 100;
    // Never materialise an arbitrarily large PEL in one Node-RED message.
    node.maxMessages = Math.max(node.count, parseInt(config.maxMessages, 10) || 1000);

    node.on('input', async function (msg, send, done) {
      send = send || function () { node.send.apply(node, arguments); };
      try {
        const streamKey = msg.stream || node.streamKey;
        const group = msg.group || node.group;
        // Той самий принцип, що й у stream-in: якщо consumer не заданий явно
        // ні в конфігурації ноди, ні через msg — падаємо на HOSTNAME поду,
        // а не залишаємо порожнім (інакше кожен под HPA-пулу впаде в error,
        // якщо оператор забув заповнити поле вручну).
        const consumer = msg.consumer || node.consumer
          || `${process.env.HOSTNAME || RED.settings.get('flowfile') || 'nr'}-${node.id}`;
        const minIdleMs = msg.minIdleMs !== undefined ? msg.minIdleMs : node.minIdleMs;

        if (!streamKey || !group || !consumer) {
          throw new Error('Stream key, group and consumer are required');
        }

        const client = node.server.getClient();
        let cursor = '0';
        const claimed = [];

        do {
          const remaining = node.maxMessages - claimed.length;
          const res = await client.xautoclaim(
            streamKey, group, consumer, minIdleMs, cursor,
            'COUNT', Math.min(node.count, remaining)
          );
          const [nextCursor, entries] = res;
          cursor = nextCursor;
          for (const [id, flat] of entries || []) {
            claimed.push({ streamId: id, payload: parseFlatFields(flat) });
          }
        } while (cursor !== '0' && claimed.length < node.maxMessages);

        msg.payload = claimed;
        msg.stream = streamKey;
        msg.group = group;
        msg.consumer = consumer;
        node.status({ fill: 'green', shape: 'dot', text: `claimed ${claimed.length}${cursor !== '0' ? '+' : ''}` });
        send(msg);
        done();
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: 'error' });
        done(err);
      }
    });
  }

  RED.nodes.registerType('yroshcha-redis-stream-claim', RedisStreamClaimNode);

  // ---------------------------------------------------------------------
  // yroshcha-redis-stream-gc — прибирання "мертвих" consumer-записів.
  // На HPA-пулі кожен рестарт поду створює нового consumer (HOSTNAME
  // змінюється), старі лишаються в XINFO CONSUMERS назавжди, доки їх
  // не видалити явно через XGROUP DELCONSUMER. Видаляємо лише consumer-ів
  // з pending=0 (без ризику загубити необроблені записи) і idle довше
  // заданого порогу (щоб не зачепити щойно перезапущений, ще активний под).
  // ---------------------------------------------------------------------
  function RedisStreamGcNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.server = getServer(node, config);
    if (!node.server) return;

    node.streamKey = config.streamKey || '';
    node.group = config.group || '';
    node.minIdleMs = parseInt(config.minIdleMs, 10) || 600000; // 10 хв за замовчуванням

    node.on('input', async function (msg, send, done) {
      send = send || function () { node.send.apply(node, arguments); };
      try {
        const streamKey = msg.stream || node.streamKey;
        const group = msg.group || node.group;
        const minIdleMs = msg.minIdleMs !== undefined ? msg.minIdleMs : node.minIdleMs;

        if (!streamKey || !group) throw new Error('Stream key and group are required');

        const client = node.server.getClient();
        // XINFO CONSUMERS повертає масив "флет"-масивів на кожного consumer:
        // ['name', <name>, 'pending', <n>, 'idle', <ms>, 'inactive', <ms>]
        const consumersRaw = await client.call('XINFO', 'CONSUMERS', streamKey, group);

        const removed = [];
        const kept = [];

        for (const flat of consumersRaw) {
          const info = parseFlatFields(flat);
          const pending = parseInt(info.pending, 10) || 0;
          const idle = parseInt(info.idle, 10) || 0;

          if (pending === 0 && idle >= minIdleMs) {
            await client.call('XGROUP', 'DELCONSUMER', streamKey, group, info.name);
            removed.push({ name: info.name, idleMs: idle });
          } else {
            kept.push({ name: info.name, pending, idleMs: idle });
          }
        }

        msg.payload = { removed, kept };
        msg.stream = streamKey;
        msg.group = group;
        node.status({ fill: 'green', shape: 'dot', text: `removed ${removed.length}, kept ${kept.length}` });
        send(msg);
        done();
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: 'error' });
        done(err);
      }
    });
  }

  RED.nodes.registerType('yroshcha-redis-stream-gc', RedisStreamGcNode);

  // ---------------------------------------------------------------------
  // yroshcha-redis-stream-metrics — lightweight observability snapshot.
  // Trigger from Inject/scheduler and forward msg.payload to the monitoring
  // flow of choice (Prometheus, OpenTelemetry, HTTP, etc.).
  // ---------------------------------------------------------------------
  function RedisStreamMetricsNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.server = getServer(node, config);
    if (!node.server) return;

    node.streamKey = config.streamKey || '';
    node.group = config.group || '';
    node.deadLetterStream = config.deadLetterStream || '';

    node.on('input', async function (msg, send, done) {
      send = send || function () { node.send.apply(node, arguments); };
      try {
        const streamKey = msg.stream || node.streamKey;
        const group = msg.group || node.group;
        const deadLetterStream = msg.deadLetterStream || node.deadLetterStream;
        if (!streamKey || !group) throw new Error('Stream key and group are required');

        const client = node.server.getClient();
        const groups = await client.call('XINFO', 'GROUPS', streamKey);
        const groupInfo = (groups || []).map(parseFlatFields).find((info) => info.name === group);
        if (!groupInfo) throw new Error(`Consumer group "${group}" does not exist`);
        const pendingSummary = await client.xpending(streamKey, group);
        const pending = Number(pendingSummary && pendingSummary[0]) || 0;
        const deadLetterLength = deadLetterStream
          ? Number(await client.call('XLEN', deadLetterStream)) || 0
          : null;

        msg.payload = {
          stream: streamKey,
          group,
          pending,
          deadLetterStream: deadLetterStream || null,
          deadLetterLength,
          consumers: Number(groupInfo.consumers) || 0,
          lag: groupInfo.lag === undefined || groupInfo.lag === null ? null : Number(groupInfo.lag),
          entriesRead: groupInfo['entries-read'] === undefined ? null : Number(groupInfo['entries-read']),
          lastDeliveredId: groupInfo['last-delivered-id'] || null,
          sampledAt: new Date().toISOString()
        };
        node.status({
          fill: deadLetterLength ? 'red' : (pending ? 'yellow' : 'green'),
          shape: 'dot',
          text: deadLetterLength ? `DLQ ${deadLetterLength}` : `PEL ${pending}`
        });
        send(msg);
        done();
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: 'metrics error' });
        done(err);
      }
    });
  }

  RED.nodes.registerType('yroshcha-redis-stream-metrics', RedisStreamMetricsNode);

  // ---------------------------------------------------------------------
  // yroshcha-redis-stream-control — control every stream consumer in this
  // Node-RED runtime. Use one HTTP endpoint per replica during scale-down.
  // ---------------------------------------------------------------------
  function RedisStreamControlNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    node.on('input', async function (msg, send, done) {
      send = send || function () { node.send.apply(node, arguments); };
      try {
        const body = msg.payload && typeof msg.payload === 'object' ? msg.payload : {};
        const result = await executeStreamControl({
          action: msg.action || body.action,
          stream: msg.stream || body.stream,
          group: msg.group || body.group,
          nodeId: msg.nodeId || body.nodeId,
          drainTimeoutMs: msg.drainTimeoutMs || body.drainTimeoutMs
        });
        msg.payload = result;
        node.status({ fill: result.drained ? 'green' : 'yellow', shape: 'dot', text: `${result.action}: ${result.consumers.length} consumer(s)` });
        send(msg);
        done();
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: 'control error' });
        done(err);
      }
    });
  }

  RED.nodes.registerType('yroshcha-redis-stream-control', RedisStreamControlNode);

  // ---------------------------------------------------------------------
  // yroshcha-redis-stream-api — self-contained HTTP API. It remains disabled
  // until explicitly enabled; inside a private network token auth is optional.
  // Routes are fixed so reverse proxies/firewalls can allow-list them:
  // GET /redis/streams/status and POST /redis/streams/control.
  // ---------------------------------------------------------------------
  function RedisStreamApiNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const token = (node.credentials && node.credentials.token) || '';
    const enabled = !!config.enabled;
    const requireToken = !!config.requireToken;

    async function execute(command, emitResult) {
      const result = await executeStreamControl(command);
      node.status({ fill: result.drained ? 'green' : 'yellow', shape: 'dot', text: `${result.action}: ${result.consumers.length} consumer(s)` });
      if (emitResult) node.send({ payload: result, topic: 'redis streams api' });
      return result;
    }

    if (enabled && (!requireToken || token)) {
      streamControlApis.set(node.id, { token, requireToken, execute });
      node.status({ fill: requireToken ? 'green' : 'yellow', shape: 'dot', text: requireToken ? 'API enabled (token)' : 'API enabled (no token)' });
    } else if (!enabled) {
      node.status({ fill: 'grey', shape: 'ring', text: 'API disabled' });
    } else {
      node.status({ fill: 'red', shape: 'ring', text: 'API token required' });
      node.warn('Redis Streams API is disabled: token protection is enabled but no API token is configured');
    }

    node.on('close', function (done) {
      streamControlApis.delete(node.id);
      done();
    });
  }

  RED.nodes.registerType('yroshcha-redis-stream-api', RedisStreamApiNode, {
    credentials: { token: { type: 'password' } }
  });

  if (RED.httpNode) {
    RED.httpNode.get('/redis/streams/status', async function (req, res) {
      const api = getEnabledControlApi(req, res);
      if (!api) return;
      try {
        res.json(await api.execute({ action: 'status' }, true));
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });
    RED.httpNode.post('/redis/streams/control', async function (req, res) {
      const api = getEnabledControlApi(req, res);
      if (!api) return;
      try {
        const body = parseHttpBody(req.body);
        res.json(await api.execute({
          action: body.action,
          stream: body.stream,
          group: body.group,
          nodeId: body.nodeId,
          drainTimeoutMs: body.drainTimeoutMs
        }, true));
      } catch (err) {
        res.status(400).json({ error: err.message });
      }
    });
  }
};
