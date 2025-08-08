const { createClient } = require('redis');
const { PriorityLockQueue } = require('../dist');

(async () => {
  const redisClient = createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
  redisClient.on('error', (err) => console.error('[redis] client error', err));

  const queue = new PriorityLockQueue({
    redisClient,
    namespace: process.env.QUEUE_NAMESPACE || 'demo',
    lockTtlMs: 15000,
    idleSleepMs: 300,
  });

  // Example handler: simulate work
  async function handler(task) {
    const ms = Math.floor(Math.random() * 2000) + 200;
    console.log(`[worker] processing id=${task.id} priority=${task.priority} sleep=${ms}ms payload=`, task.payload);
    await new Promise((r) => setTimeout(r, ms));

    // Randomly fail to demonstrate failure handling
    if (Math.random() < 0.2) {
      throw new Error('Random failure');
    }

    console.log(`[worker] done id=${task.id}`);
  }

  process.on('SIGINT', () => {
    console.log('Stopping worker...');
    queue.stopWorker();
  });

  await queue.startWorker(handler, { maxAttempts: 2, batchSize: 1 });
})();