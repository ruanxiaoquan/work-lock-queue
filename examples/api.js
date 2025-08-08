// 示例：基于 Express 暴露队列 API（入队、指标、观测）
const express = require('express');
const { createClient } = require('redis');
// 从已编译的 dist 导入队列实现（生产环境应引用包入口）
const { PriorityLockQueue } = require('../dist');

const app = express();
app.use(express.json()); // 解析 JSON 请求体

// 创建 Redis 客户端（可通过 REDIS_URL 配置连接串）
const redisClient = createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
redisClient.on('error', (err) => console.error('[redis] client error', err));

// 创建队列实例
const queue = new PriorityLockQueue({
  redisClient, // 复用同一个 redis 连接
  namespace: process.env.QUEUE_NAMESPACE || 'demo', // 命名空间前缀，便于隔离环境
});

// 入队接口：POST /task  body: { payload, priority }
app.post('/task', async (req, res) => {
  try {
    const payload = req.body?.payload ?? req.body ?? {};
    const priority = Number(req.query.priority ?? req.body?.priority ?? 5); // 数字越小优先级越高
    const id = await queue.enqueueTask(payload, priority);
    res.json({ id, priority });
  } catch (err) {
    res.status(500).json({ error: err?.message || String(err) });
  }
});

// 指标接口：扫描 *:pending 推断所有命名空间并汇总基本指标
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
        processingCount: Number(processingCount || 0), // 当前并发占用（运行中任务数）
        failedCount: Number(failedCount || 0),
      });
    }

    res.json({ namespaces: Array.from(namespaces), metrics: results });
  } catch (err) {
    res.status(500).json({ error: err?.message || String(err) });
  }
});

// 批量命名空间的指标：GET /metrics/:namespaces  例如 /metrics/ns1,ns2
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

// 观测接口：返回运行中/失败/成功的任务详情（可指定多命名空间）
app.get(['/observe', '/observe/:namespaces'], async (req, res) => {
  try {
    const limit = Number(req.query.limit || 100);

    let namespaces = [];
    if (req.params.namespaces) {
      namespaces = String(req.params.namespaces).split(',').filter(Boolean);
    } else if (req.query.namespaces) {
      namespaces = String(req.query.namespaces).split(',').filter(Boolean);
    } else {
      // 若未指定，则自动发现
      const discovered = new Set();
      for await (const key of redisClient.scanIterator({ MATCH: '*:pending', COUNT: 100 })) {
        discovered.add(String(key).replace(/:pending$/, ''));
      }
      namespaces = Array.from(discovered);
    }

    const results = [];

    for (const ns of namespaces) {
      // 正在运行：processing hash 中的条目 -> 拉取任务详情
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

      // 失败列表
      const failedRaw = await redisClient.lRange(`${ns}:failed`, 0, limit - 1);
      const failed = failedRaw.map((s) => safeParse(s));

      // 成功列表
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

// 辅助：安全解析 JSON 字符串
function safeParse(str) {
  if (typeof str !== 'string') return str;
  try {
    return JSON.parse(str);
  } catch {
    return str;
  }
}

// 启动服务
const port = process.env.PORT || 3000;
(async () => {
  await redisClient.connect();
  app.listen(port, () => {
    console.log(`API listening on http://localhost:${port} (POST /task)`);
    console.log('Metrics endpoints: GET /metrics, GET /metrics/:namespaces');
    console.log('Observation endpoints: GET /observe, GET /observe/:namespaces?limit=100');
  });
})();