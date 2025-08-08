"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PriorityLockQueue = exports.TaskPriority = void 0;
const uuid_1 = require("uuid");
var TaskPriority;
(function (TaskPriority) {
    TaskPriority[TaskPriority["DEFAULT"] = 1] = "DEFAULT";
    TaskPriority[TaskPriority["MEDIUM"] = 2] = "MEDIUM";
    TaskPriority[TaskPriority["HIGH"] = 3] = "HIGH";
})(TaskPriority || (exports.TaskPriority = TaskPriority = {}));
class PriorityLockQueue {
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
        const clientAsAny = this.client;
        if (clientAsAny && typeof clientAsAny.on === 'function') {
            clientAsAny.on('error', (err) => this.log.error('[redis] client error', err));
        }
        this.workerAbort = false;
        this.currentLockValue = null;
        this.didConnectInternally = false;
    }
    async connect() {
        if (!this.client.isOpen) {
            await this.client.connect();
            this.didConnectInternally = true;
        }
    }
    async disconnect() {
        if (this.didConnectInternally) {
            await this.client.quit();
            this.didConnectInternally = false;
        }
    }
    static computeScore(priority, nowMs) {
        const numericPriority = Number.isFinite(priority) ? Number(priority) : 5;
        const safePriority = Math.max(0, Math.min(1000, Math.floor(numericPriority)));
        const PRIORITY_SCALE = 1e12;
        return safePriority * PRIORITY_SCALE + nowMs;
    }
    taskKey(taskId) {
        return `${this.namespace}:task:${taskId}`;
    }
    // Expose current namespace
    getNamespace() {
        return this.namespace;
    }
    // Fetch queue metrics for this namespace
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
    async enqueueTask(payload, priority) {
        await this.connect();
        const taskId = (0, uuid_1.v4)();
        const createdAtMs = Date.now();
        const score = PriorityLockQueue.computeScore(priority, createdAtMs);
        const payloadString = typeof payload === 'string' ? payload : JSON.stringify(payload);
        const taskHashKey = this.taskKey(taskId);
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
    async sleep(ms) {
        await new Promise((resolve) => setTimeout(resolve, ms));
    }
    async startWorker(handler, options = {}) {
        await this.connect();
        const { maxAttempts = 3, renewIntervalMs = Math.max(1000, Math.floor(this.lockTtlMs / 3)), batchSize = 1, concurrency = 1, } = options;
        this.workerAbort = false;
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
                const startNextIfAny = async () => {
                    // Pop one task and start processing if available
                    const popped = (await this.client.zPopMin(this.keys.pending));
                    if (!popped)
                        return false;
                    const raw = Array.isArray(popped) ? popped[0] : popped;
                    const taskId = typeof raw === 'string' ? raw : raw.value;
                    const taskKey = this.taskKey(taskId);
                    const taskData = await this.client.hGetAll(taskKey);
                    if (!taskData || !taskData.id) {
                        return true; // consider as consumed; try to continue
                    }
                    const deserialized = {
                        id: taskData.id,
                        payload: this.safeParse(taskData.payload),
                        priority: Number(taskData.priority || 5),
                        createdAtMs: Number(taskData.createdAtMs || Date.now()),
                        attempts: Number(taskData.attempts || 0),
                    };
                    await this.client.hSet(this.keys.processing, taskId, String(Date.now()));
                    const p = (async () => {
                        try {
                            await handler(deserialized);
                            // On success, record to succeeded list with timing, then cleanup
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
                        // Should not happen due to try/catch inside, but guard just in case
                        this.log.error('[worker] unhandled task error', err);
                    })
                        .finally(() => {
                        inflight.delete(p);
                    });
                    inflight.add(p);
                    startedCount += 1;
                    return true;
                };
                // Fill up initial concurrency window
                while (!this.workerAbort &&
                    inflight.size < concurrency &&
                    startedCount < batchSize) {
                    const didStart = await startNextIfAny();
                    if (!didStart)
                        break;
                }
                // While there are still tasks running, as each finishes, try to start next until batchSize reached
                while (!this.workerAbort && inflight.size > 0) {
                    // Wait for any one to settle
                    await Promise.race(Array.from(inflight));
                    // Refill slots
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
    stopWorker() {
        this.workerAbort = true;
    }
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