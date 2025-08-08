// 示例：启动 worker 管理器，自动发现命名空间并按需启动 worker
const { createClient } = require('redis');
const { QueueWorkerManager } = require('../dist');

(async () => {
  // 连接 Redis（可通过 REDIS_URL 配置）
  const redisClient = createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
  redisClient.on('error', (err) => console.error('[redis] client error', err));
  await redisClient.connect();

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

  const manager = QueueWorkerManager.getInstance();

  // 启动管理器：
  // - 自动扫描一次并为每个命名空间启动 worker；
  // - 每 10 秒自动重扫一次；
  // - 监听控制通道 `queue:worker:control` 的 start/stop/rescan 事件。
  await manager.start(handler, {
    redisClient,
    // 可按命名空间配置并发；未配置的命名空间默认 5
    workerOptions: { maxAttempts: 2, batchSize: 20, concurrency: { [process.env.QUEUE_NAMESPACE || 'demo']: Number(process.env.CONCURRENCY || 5) } },
    queueOptions: { lockTtlMs: 15000, idleSleepMs: 300 },
    scanIntervalMs: Number(process.env.SCAN_INTERVAL_MS || 10000),
    controlChannel: process.env.CONTROL_CHANNEL || 'queue:worker:control',
  });

  // 支持 Ctrl+C 安全停止
  process.on('SIGINT', async () => {
    console.log('Stopping worker manager...');
    await manager.stop();
    try { await redisClient.quit(); } catch {}
    process.exit(0);
  });
})();