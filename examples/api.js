const express = require('express');
const { PriorityLockQueue } = require('../src');

const app = express();
app.use(express.json());

const queue = new PriorityLockQueue({
  namespace: process.env.QUEUE_NAMESPACE || 'demo',
  redisUrl: process.env.REDIS_URL || 'redis://localhost:6379',
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