const { PriorityLockQueue } = require('../src');

(async () => {
  const queue = new PriorityLockQueue({
    namespace: process.env.QUEUE_NAMESPACE || 'demo',
    redisUrl: process.env.REDIS_URL || 'redis://localhost:6379',
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