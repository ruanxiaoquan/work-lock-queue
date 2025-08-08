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

// Metrics for a single namespace
app.get('/metrics/:namespace', async (req, res) => {
  const ns = req.params.namespace;
  try {
    const [pendingCount, processingCount, failedCount] = await Promise.all([
      redisClient.zCard(`${ns}:pending`),
      redisClient.hLen(`${ns}:processing`),
      redisClient.lLen(`${ns}:failed`),
    ]);
    res.json({
      namespace: ns,
      pendingCount: Number(pendingCount || 0),
      processingCount: Number(processingCount || 0), // current concurrency usage
      failedCount: Number(failedCount || 0),
    });
  } catch (err) {
    res.status(500).json({ error: err?.message || String(err) });
  }
});

const port = process.env.PORT || 3000;
(async () => {
  await redisClient.connect();
  app.listen(port, () => {
    console.log(`API listening on http://localhost:${port} (POST /task)`);
    console.log('Metrics endpoints: GET /metrics, GET /metrics/:namespace');
  });
})();