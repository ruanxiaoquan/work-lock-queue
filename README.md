# Redis 优先级锁队列（Node.js）

一个基于 Redis 的队列，实现“分布式锁 + 优先级 + 失败处理”。通过抢占式分布式锁保证同一时刻只有一台实例批量拉取任务；任务按优先级（数值越小优先级越高）入队与调度；失败任务会记录到失败列表并按配置进行重试。

- **单一生产者 API**：`enqueueTask(payload, priority)` 将任务入队（按优先级排序）
- **Worker 并发**：通过分布式锁控制实例间互斥，每轮在持锁窗口内按 `concurrency` 并发处理
- **失败处理**：失败会写入失败列表，并在未超过 `maxAttempts` 时重新入队
- **优先级说明**：数字越小优先级越高（0 最高），同优先级按入队时间 FIFO

## 安装

```bash
npm install
```

如不是本机 Redis，请设置环境变量 `REDIS_URL`。

## 用法

入队任务示例：

```js
const { PriorityLockQueue } = require('./src');

(async () => {
  const queue = new PriorityLockQueue({ namespace: 'jobs', redisClient /* 传入 redis 客户端 */ });
  await queue.enqueueTask({ userId: 123 }, 0); // 最高优先级
  await queue.enqueueTask({ userId: 456 }, 5); // 普通优先级
})();
```

运行 worker：

```bash
CONCURRENCY=5 npm run start:worker
```

环境变量：

- `REDIS_URL`（默认 `redis://localhost:6379`）
- `QUEUE_NAMESPACE`（默认 `demo`）

## Worker 管理器（自动发现命名空间 + 事件驱动启动）

引入 `QueueWorkerManager` 单例，自动识别当前所有命名空间（基于 `*:pending` 扫描），为每个命名空间自动实例化并启动 worker 主循环；并通过 Redis Pub/Sub 监听控制事件以按需启动/停止，避免重复监听与重复启动。

示例（见 `examples/worker.js`）：

```js
const { createClient } = require('redis');
const { QueueWorkerManager } = require('./dist');

(async () => {
  const redisClient = createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
  await redisClient.connect();

  async function handler(task) {
    // 处理任务...
  }

  const manager = QueueWorkerManager.getInstance();
  await manager.start(handler, {
    redisClient,
    workerOptions: { maxAttempts: 2, batchSize: 20, concurrency: 5 },
    queueOptions: { lockTtlMs: 15000, idleSleepMs: 300 },
    scanIntervalMs: 10000, // 周期性重扫，发现新命名空间
    controlChannel: 'queue:worker:control',
  });
})();
```

控制通道（Pub/Sub）消息格式：

- 启动指定命名空间：`{"action":"start","namespace":"ns1"}`
- 停止指定命名空间：`{"action":"stop","namespace":"ns1"}`
- 触发重新扫描：`{"action":"rescan"}`

发布示例（redis-cli）：

```bash
redis-cli PUBLISH queue:worker:control '{"action":"start","namespace":"demo"}'
```

## 工作原理

- 待处理队列：`namespace:pending`（ZSET）。score = `priority * 1e12 + createdAtMs`，分数越小越先出队
- 任务元信息：`namespace:task:{id}`（HASH）
- 失败队列：`namespace:failed`（LIST）
- 成功队列：`namespace:succeeded`（LIST）
- 分布式锁：`namespace:lock`，通过 `SET NX PX` 获取，Lua 脚本安全续约与释放

Worker 主循环：

1. 获取分布式锁
2. 从待处理队列拉取至多 `batchSize` 个任务，并以最多 `concurrency` 并发处理
3. 执行用户提供的 `handler`
4. 成功：记录到成功列表并清理任务元信息
5. 失败：记录到失败列表，若未超过 `maxAttempts` 则重新入队
6. 当前轮结束后释放锁；若无任务则短暂休眠，避免热循环

## 示例

- 入队演示：`npm run start:demo`
- 启动 worker：`npm run start:worker`
- 启动 API：`npm run start:api`

## 重要说明与参数

`startWorker(handler, { concurrency, batchSize, maxAttempts, renewIntervalMs })`

- **concurrency**：单轮最大并发数（默认 1）
- **batchSize**：单轮最多拉取的任务数（默认 1），应 ≥ `concurrency` 以充分利用并发
- **maxAttempts**：最大重试次数（不含首次，默认 3）
- **renewIntervalMs**：锁续约间隔（默认 `max(1000, lockTtlMs/3)`）

`PriorityLockQueue` 构造参数：

- **redisClient**：Redis 客户端（必填）
- **namespace**：命名空间前缀（默认 `queue`）
- **lockTtlMs**：锁过期时间（毫秒，默认 `30000`）
- **idleSleepMs**：无锁/无任务时的休眠（毫秒，默认 `500`）
- **log**：日志对象，默认 `console`

## 观测与排障

- 指标：`GET /metrics` 或 `GET /metrics/:namespaces`（示例 API，含 pending/processing/failed/succeeded 数量）
- 观测：`GET /observe` 或 `GET /observe/:namespaces?limit=100`，也可使用 `?namespace=ns` 单个过滤
- 运行中：`namespace:processing`（HASH）
- 失败记录：`namespace:failed`（LIST）
- 成功记录：`namespace:succeeded`（LIST）

## 备注

- 优先级数值越小越先执行（0 最高）。同一优先级下按入队时间先后执行（FIFO）
- 锁会在处理期间后台持续续约，避免长任务期间锁过期
- 建议 `batchSize >= concurrency`，否则并发无法完全利用