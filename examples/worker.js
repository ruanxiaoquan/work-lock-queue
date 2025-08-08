// 示例：启动一个 worker 处理队列任务
const { createClient } = require('redis');
const { PriorityLockQueue } = require('../dist');

(async () => {
  // 连接 Redis（可通过 REDIS_URL 配置）
  const redisClient = createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
  redisClient.on('error', (err) => console.error('[redis] client error', err));

  // 创建队列实例并配置
  const queue = new PriorityLockQueue({
    redisClient,
    namespace: process.env.QUEUE_NAMESPACE || 'demo', // 命名空间，区分不同业务/环境
    lockTtlMs: 15000, // 分布式锁过期时间（毫秒）
    idleSleepMs: 300, // 无锁/无任务时的休眠时间（毫秒）
  });

  // 任务处理函数（用户自定义）
  async function handler(task) {
    const ms = Math.floor(Math.random() * 2000) + 200;
    console.log(`[worker] processing id=${task.id} priority=${task.priority} sleep=${ms}ms payload=`, task.payload);
    await new Promise((r) => setTimeout(r, ms));

    // 模拟随机失败，演示失败记录与重试
    if (Math.random() < 0.2) {
      throw new Error('Random failure');
    }

    console.log(`[worker] done id=${task.id}`);
  }

  // 支持 Ctrl+C 安全停止
  process.on('SIGINT', () => {
    console.log('Stopping worker...');
    queue.stopWorker();
  });

  // 启动 worker
  await queue.startWorker(handler, { maxAttempts: 2, batchSize: 20, concurrency: Number(process.env.CONCURRENCY || 5) });
})();