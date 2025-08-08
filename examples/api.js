const express = require('express');
const { createClient } = require('redis');
const { PriorityLockQueue } = require('../dist');

const app = express();
app.use(express.json());

const redisClient = createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
redisClient.on('error', (err) => console.error('[redis] client error', err));

const queue = new PriorityLockQueue({
  redisClient,
  namespace: process.env.QUEUE_NAMESPACE || 'demo',
});

app.post('/task', async (req, res) => {
  try {
    const payload = req.body?.payload ?? req.body ?? {};
    const priority = Number(req.query.priority ?? req.body?.priority ?? 5);
    const id = await queue.enqueueTask(payload, priority);
    res.json({ id, priority });
  } catch (err) {
    res.status(500).json({ error: err?.message || String(err) });
  }
});

// List metrics for all namespaces discovered by scanning *:pending keys
app.get('/metrics', async (req, res) => {
  try {
    const namespaces = new Set();
    for await (const key of redisClient.scanIterator({ MATCH: '*:pending', COUNT: 100 })) {
      const ns = String(key).replace(/:pending$/, '');
      namespaces.add(ns);
    }

    const results = [];
    for (const ns of namespaces) {
      const [pendingCount, processingCount, failedCount] = await Promise.all([
        redisClient.zCard(`${ns}:pending`),
        redisClient.hLen(`${ns}:processing`),
        redisClient.lLen(`${ns}:failed`),
      ]);
      results.push({
        namespace: ns,
        pendingCount: Number(pendingCount || 0),
        processingCount: Number(processingCount || 0), // current concurrency usage
        failedCount: Number(failedCount || 0),
      });
    }

    res.json({ namespaces: Array.from(namespaces), metrics: results });
  } catch (err) {
    res.status(500).json({ error: err?.message || String(err) });
  }
});

// Metrics for single or multiple namespaces: GET /metrics/:namespaces where :namespaces can be "ns1,ns2"
app.get('/metrics/:namespaces', async (req, res) => {
  const list = String(req.params.namespaces || '').split(',').filter(Boolean);
  try {
    const results = [];
    for (const ns of list) {
      const [pendingCount, processingCount, failedCount] = await Promise.all([
        redisClient.zCard(`${ns}:pending`),
        redisClient.hLen(`${ns}:processing`),
        redisClient.lLen(`${ns}:failed`),
      ]);
      results.push({
        namespace: ns,
        pendingCount: Number(pendingCount || 0),
        processingCount: Number(processingCount || 0),
        failedCount: Number(failedCount || 0),
      });
    }
    res.json({ metrics: results });
  } catch (err) {
    res.status(500).json({ error: err?.message || String(err) });
  }
});

// Observation API: detailed running/failed/succeeded for one or many namespaces
app.get(['/observe', '/observe/:namespaces'], async (req, res) => {
  try {
    const limit = Number(req.query.limit || 100);

    let namespaces = [];
    if (req.params.namespaces) {
      namespaces = String(req.params.namespaces).split(',').filter(Boolean);
    } else if (req.query.namespaces) {
      namespaces = String(req.query.namespaces).split(',').filter(Boolean);
    } else {
      // discover from pending keys if not specified
      const discovered = new Set();
      for await (const key of redisClient.scanIterator({ MATCH: '*:pending', COUNT: 100 })) {
        discovered.add(String(key).replace(/:pending$/, ''));
      }
      namespaces = Array.from(discovered);
    }

    const results = [];

    for (const ns of namespaces) {
      // running: from processing hash -> fetch task details
      const processingEntries = await redisClient.hGetAll(`${ns}:processing`);
      const running = [];
      for (const [taskId, startedAtStr] of Object.entries(processingEntries)) {
        const task = await redisClient.hGetAll(`${ns}:task:${taskId}`);
        if (task && task.id) {
          running.push({
            id: task.id,
            payload: safeParse(task.payload),
            priority: Number(task.priority || 5),
            createdAtMs: Number(task.createdAtMs || Date.now()),
            attempts: Number(task.attempts || 0),
            startedAtMs: Number(startedAtStr || Date.now()),
          });
        } else {
          running.push({ id: taskId, startedAtMs: Number(startedAtStr || Date.now()) });
        }
      }

      // failed: from failed list
      const failedRaw = await redisClient.lRange(`${ns}:failed`, 0, limit - 1);
      const failed = failedRaw.map((s) => safeParse(s));

      // succeeded: from succeeded list
      const succeededRaw = await redisClient.lRange(`${ns}:succeeded`, 0, limit - 1);
      const succeeded = succeededRaw.map((s) => safeParse(s));

      results.push({
        namespace: ns,
        running,
        failed,
        succeeded,
      });
    }

    res.json({ results });
  } catch (err) {
    res.status(500).json({ error: err?.message || String(err) });
  }
});

function safeParse(str) {
  if (typeof str !== 'string') return str;
  try {
    return JSON.parse(str);
  } catch {
    return str;
  }
}

const port = process.env.PORT || 3000;
(async () => {
  await redisClient.connect();
  app.listen(port, () => {
    console.log(`API listening on http://localhost:${port} (POST /task)`);
    console.log('Metrics endpoints: GET /metrics, GET /metrics/:namespaces');
    console.log('Observation endpoints: GET /observe, GET /observe/:namespaces?limit=100');
  });
})();