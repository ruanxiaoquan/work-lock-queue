const { createClient } = require('redis');
const { PriorityLockQueue } = require('../dist');

(async () => {
  const redisClient = createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
  redisClient.on('error', (err) => console.error('[redis] client error', err));

  const queue = new PriorityLockQueue({
    redisClient,
    namespace: process.env.QUEUE_NAMESPACE || 'demo',
  });

  await queue.connect();

  const jobs = [];
  for (let i = 0; i < 10; i++) {
    const priority = i % 3; // 0,1,2 with 0 highest
    const id = await queue.enqueueTask({ index: i }, priority);
    console.log(`[enqueue] id=${id} priority=${priority}`);
    jobs.push(id);
  }

  await queue.disconnect();
})();