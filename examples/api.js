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

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`API listening on http://localhost:${port} (POST /task)`);
});