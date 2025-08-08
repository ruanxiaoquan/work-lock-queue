"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PriorityLockQueue = exports.TaskPriority = void 0;
const uuid_1 = require("uuid");
/**
 * 任务优先级枚举（数字越小，优先级越高）。
 * 建议范围：0（最高）~ 1000（最低）。
 */
var TaskPriority;
(function (TaskPriority) {
    /** 默认优先级（1），高于 MEDIUM/HIGH 的语义仅作示例，数字越小越先处理 */
    TaskPriority[TaskPriority["DEFAULT"] = 1] = "DEFAULT";
    /** 中等优先级（2） */
    TaskPriority[TaskPriority["MEDIUM"] = 2] = "MEDIUM";
    /** 较高优先级（3），注意数值越小优先级越高，0 最高 */
    TaskPriority[TaskPriority["HIGH"] = 3] = "HIGH";
})(TaskPriority || (exports.TaskPriority = TaskPriority = {}));
/**
 * 基于 Redis 的带优先级、分布式锁的队列。
 * - 仅持有锁的 worker 才能批量拉取并处理任务；
 * - 优先级越高（数字越小）越先被处理；
 * - 失败任务会被记录到失败列表，并按配置重试；
 * - 提供基本指标与观测支持（processing/failed/succeeded）。
 */
class PriorityLockQueue {
    /**
     * 构造函数。
     * @param options 配置项（见 PriorityLockQueueOptions）
     */
    constructor(options) {
        const { redisClient, namespace = 'queue', lockTtlMs = 30000, idleSleepMs = 500, log = console, } = options;
        this.client = redisClient;
        this.namespace = namespace;
        this.keys = {
            pending: `${namespace}:pending`,
            failed: `${namespace}:failed`,
            processing: `${namespace}:processing`,
            lock: `${namespace}:lock`,
            succeeded: `${namespace}:succeeded`,
        };
        this.lockTtlMs = lockTtlMs;
        this.idleSleepMs = idleSleepMs;
        this.log = log;
        // 监听 Redis 客户端错误，便于排查问题
        const clientAsAny = this.client;
        if (clientAsAny && typeof clientAsAny.on === 'function') {
            clientAsAny.on('error', (err) => this.log.error('[redis] client error', err));
        }
        this.workerAbort = false;
        this.currentLockValue = null;
        this.didConnectInternally = false;
    }
    /**
     * 确保 Redis 已连接。
     * - 如果外部尚未连接，则在此连接，并记录内部连接标记
     */
    async connect() {
        if (!this.client.isOpen) {
            await this.client.connect();
            this.didConnectInternally = true;
        }
    }
    /**
     * 关闭 Redis 连接（仅当 connect() 是内部打开时）。
     */
    async disconnect() {
        if (this.didConnectInternally) {
            await this.client.quit();
            this.didConnectInternally = false;
        }
    }
    /**
     * 计算任务的排序分数（score）。
     * 分数 = priority * 1e12 + nowMs，数值越小越先被处理。
     */
    static computeScore(priority, nowMs) {
        const numericPriority = Number.isFinite(priority) ? Number(priority) : 5;
        const safePriority = Math.max(0, Math.min(1000, Math.floor(numericPriority)));
        const PRIORITY_SCALE = 1e12;
        return safePriority * PRIORITY_SCALE + nowMs;
    }
    /**
     * 获取任务在 Redis 中的 HASH key。
     */
    taskKey(taskId) {
        return `${this.namespace}:task:${taskId}`;
    }
    /** 暴露当前命名空间 */
    getNamespace() {
        return this.namespace;
    }
    /**
     * 获取队列当前指标（pending/processing/failed 数量）。
     */
    async getMetrics() {
        const [pendingCount, processingCount, failedCount] = (await Promise.all([
            this.client.zCard(this.keys.pending),
            this.client.hLen(this.keys.processing),
            this.client.lLen(this.keys.failed),
        ]));
        return {
            namespace: this.namespace,
            pendingCount: Number(pendingCount || 0),
            processingCount: Number(processingCount || 0),
            failedCount: Number(failedCount || 0),
        };
    }
    /**
     * 入队一个任务。
     * @param payload 任务载荷（对象将被 JSON 序列化存储）
     * @param priority 优先级（数字越小越高），默认 TaskPriority.DEFAULT
     * @returns 任务 ID
     */
    async enqueueTask(payload, priority) {
        await this.connect();
        const taskId = (0, uuid_1.v4)();
        const createdAtMs = Date.now();
        const score = PriorityLockQueue.computeScore(priority, createdAtMs);
        const payloadString = typeof payload === 'string' ? payload : JSON.stringify(payload);
        const taskHashKey = this.taskKey(taskId);
        // 事务：写入任务 HASH、加入待处理 ZSET、设置过期
        const multi = this.client.multi();
        multi.hSet(taskHashKey, {
            id: taskId,
            payload: payloadString,
            priority: String(priority),
            createdAtMs: String(createdAtMs),
            attempts: '0',
        });
        multi.zAdd(this.keys.pending, [{ score, value: taskId }]);
        multi.expire(taskHashKey, 7 * 24 * 60 * 60);
        await multi.exec();
        return taskId;
    }
    /**
     * 尝试获取分布式锁（SET NX PX）。
     * 成功返回 true，并记录当前锁值用于续约/释放。
     */
    async acquireLock() {
        const lockValue = (0, uuid_1.v4)();
        const ok = await this.client.set(this.keys.lock, lockValue, {
            NX: true,
            PX: this.lockTtlMs,
        });
        if (ok) {
            this.currentLockValue = lockValue;
            return true;
        }
        return false;
    }
    /**
     * 续约分布式锁（Lua 校验锁值一致后 pexpire）。
     */
    async renewLock() {
        if (!this.currentLockValue)
            return false;
        const script = `
      if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('pexpire', KEYS[1], ARGV[2])
      else
        return 0
      end
    `;
        const result = await this.client.eval(script, {
            keys: [this.keys.lock],
            arguments: [this.currentLockValue, String(this.lockTtlMs)],
        });
        return Number(result) === 1;
    }
    /**
     * 释放分布式锁（Lua 校验锁值一致后 del）。
     */
    async releaseLock() {
        if (!this.currentLockValue)
            return false;
        const script = `
      if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
      else
        return 0
      end
    `;
        const result = await this.client.eval(script, {
            keys: [this.keys.lock],
            arguments: [this.currentLockValue],
        });
        const released = Number(result) === 1;
        this.currentLockValue = null;
        return released;
    }
    /**
     * Promise 版 sleep，worker 空转时使用以避免热循环。
     */
    async sleep(ms) {
        await new Promise((resolve) => setTimeout(resolve, ms));
    }
    /**
     * 启动 worker 主循环。
     * - 仅持有锁的实例会拉取任务；
     * - 每轮最多拉取 batchSize 个任务，且最多并发 concurrency 个执行；
     * - 失败按 maxAttempts 控制是否重试，失败记录写入失败列表；
     * - 期间后台协程按 renewIntervalMs 定期续约锁。
     */
    async startWorker(handler, options = {}) {
        await this.connect();
        const { maxAttempts = 3, renewIntervalMs = Math.max(1000, Math.floor(this.lockTtlMs / 3)), batchSize = 1, concurrency = 1, } = options;
        this.workerAbort = false;
        // 后台锁续约协程
        const renewer = async () => {
            while (!this.workerAbort) {
                if (this.currentLockValue) {
                    try {
                        await this.renewLock();
                    }
                    catch (err) {
                        this.log.warn('[lock] renew failed', err);
                    }
                }
                await this.sleep(renewIntervalMs);
            }
        };
        void renewer();
        while (!this.workerAbort) {
            let hasLock = false;
            try {
                hasLock = await this.acquireLock();
            }
            catch (err) {
                this.log.error('[lock] acquire error', err);
            }
            if (!hasLock) {
                await this.sleep(this.idleSleepMs);
                continue;
            }
            try {
                let startedCount = 0;
                const inflight = new Set();
                // 拉取并启动下一个任务（若有）
                const startNextIfAny = async () => {
                    // 从 ZSET 弹出一个最小 score 的任务
                    const popped = (await this.client.zPopMin(this.keys.pending));
                    if (!popped)
                        return false;
                    const raw = Array.isArray(popped) ? popped[0] : popped;
                    const taskId = typeof raw === 'string' ? raw : raw.value;
                    const taskKey = this.taskKey(taskId);
                    const taskData = await this.client.hGetAll(taskKey);
                    if (!taskData || !taskData.id) {
                        return true; // 视为已消费，尝试继续
                    }
                    const deserialized = {
                        id: taskData.id,
                        payload: this.safeParse(taskData.payload),
                        priority: Number(taskData.priority || 5),
                        createdAtMs: Number(taskData.createdAtMs || Date.now()),
                        attempts: Number(taskData.attempts || 0),
                    };
                    // 标记为正在处理
                    await this.client.hSet(this.keys.processing, taskId, String(Date.now()));
                    const p = (async () => {
                        try {
                            await handler(deserialized);
                            // 成功：记录成功信息并清理元数据
                            const startedAtMsStr = await this.client.hGet(this.keys.processing, taskId);
                            const succeededAtMs = Date.now();
                            const successRecord = JSON.stringify({
                                id: deserialized.id,
                                payload: deserialized.payload,
                                priority: deserialized.priority,
                                createdAtMs: deserialized.createdAtMs,
                                startedAtMs: Number(startedAtMsStr || succeededAtMs),
                                succeededAtMs,
                                attempts: deserialized.attempts,
                            });
                            const multi = this.client.multi();
                            multi.lPush(this.keys.succeeded, successRecord);
                            multi.hDel(this.keys.processing, taskId);
                            multi.del(taskKey);
                            await multi.exec();
                        }
                        catch (err) {
                            // 失败：增加尝试次数并根据策略重试或仅记录
                            deserialized.attempts += 1;
                            await this.client.hSet(taskKey, 'attempts', String(deserialized.attempts));
                            const failureRecord = JSON.stringify({
                                id: deserialized.id,
                                payload: deserialized.payload,
                                priority: deserialized.priority,
                                createdAtMs: deserialized.createdAtMs,
                                failedAtMs: Date.now(),
                                attempts: deserialized.attempts,
                                error: this.serializeError(err),
                            });
                            if (deserialized.attempts < maxAttempts) {
                                const score = PriorityLockQueue.computeScore(deserialized.priority, deserialized.createdAtMs);
                                const multi = this.client.multi();
                                multi.lPush(this.keys.failed, failureRecord);
                                multi.zAdd(this.keys.pending, [
                                    { score, value: deserialized.id },
                                ]);
                                multi.hDel(this.keys.processing, taskId);
                                await multi.exec();
                            }
                            else {
                                const multi = this.client.multi();
                                multi.lPush(this.keys.failed, failureRecord);
                                multi.hDel(this.keys.processing, taskId);
                                await multi.exec();
                            }
                        }
                    })()
                        .catch((err) => {
                        // 理论上不会触发（已在 try/catch 内处理），兜底日志
                        this.log.error('[worker] unhandled task error', err);
                    })
                        .finally(() => {
                        inflight.delete(p);
                    });
                    inflight.add(p);
                    startedCount += 1;
                    return true;
                };
                // 先填满并发窗口
                while (!this.workerAbort &&
                    inflight.size < concurrency &&
                    startedCount < batchSize) {
                    const didStart = await startNextIfAny();
                    if (!didStart)
                        break;
                }
                // 等待并在空槽出现时继续拉取直至达到 batchSize
                while (!this.workerAbort && inflight.size > 0) {
                    // 等任意一个任务完成
                    await Promise.race(Array.from(inflight));
                    // 继续补充任务
                    while (!this.workerAbort &&
                        inflight.size < concurrency &&
                        startedCount < batchSize) {
                        const didStart = await startNextIfAny();
                        if (!didStart)
                            break;
                    }
                }
            }
            finally {
                await this.releaseLock();
            }
            await this.sleep(this.idleSleepMs);
        }
    }
    /**
     * 请求停止 worker（安全停止：会在当前循环点生效）。
     */
    stopWorker() {
        this.workerAbort = true;
    }
    /** 安全 JSON 解析：失败时返回原字符串 */
    safeParse(str) {
        if (typeof str !== 'string')
            return str;
        try {
            return JSON.parse(str);
        }
        catch {
            return str;
        }
    }
    /** 将错误对象序列化为可 JSON 化的结构 */
    serializeError(err) {
        if (!err)
            return { message: 'Unknown error' };
        if (err instanceof Error) {
            return { name: err.name, message: err.message, stack: err.stack };
        }
        if (typeof err === 'object')
            return err;
        return { message: String(err) };
    }
}
exports.PriorityLockQueue = PriorityLockQueue;
//# sourceMappingURL=queue.js.map