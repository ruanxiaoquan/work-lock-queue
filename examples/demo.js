// 示例：批量入队 10 个任务，演示优先级（数值越小越高）
const { createClient } = require('redis');
const { PriorityLockQueue } = require('../dist');

(async () => {
  // 连接 Redis（可通过 REDIS_URL 配置）
  const redisClient = createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
  redisClient.on('error', (err) => console.error('[redis] client error', err));

  // 创建队列实例，命名空间默认 'demo'
  const queue = new PriorityLockQueue({
    redisClient,
    namespace: process.env.QUEUE_NAMESPACE || 'demo',
  });

  await queue.connect();

  // 入队 10 个任务，优先级按 i % 3 轮换：0（最高）、1、2
  const jobs = [];
  for (let i = 0; i < 10; i++) {
    const priority = i % 3; // 0,1,2；数值越小越先执行
    const id = await queue.enqueueTask({ index: i }, priority);
    console.log(`[enqueue] id=${id} priority=${priority}`);
    jobs.push(id);
  }

  await queue.disconnect();
})();