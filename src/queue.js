const { createClient } = require('redis');
const { randomUUID } = require('crypto');

class PriorityLockQueue {
  constructor(options = {}) {
    const {
      redisUrl = process.env.REDIS_URL || 'redis://localhost:6379',
      namespace = 'queue',
      lockTtlMs = 30000,
      idleSleepMs = 500,
      log = console,
    } = options;

    this.redisUrl = redisUrl;
    this.namespace = namespace;
    this.keys = {
      pending: `${namespace}:pending`,
      failed: `${namespace}:failed`,
      processing: `${namespace}:processing`,
      lock: `${namespace}:lock`,
    };

    this.lockTtlMs = lockTtlMs;
    this.idleSleepMs = idleSleepMs;
    this.log = log;

    this.client = createClient({ url: this.redisUrl });
    this.client.on('error', (err) => this.log.error('[redis] client error', err));

    this.workerAbort = false;
    this.currentLockValue = null;
  }

  async connect() {
    if (!this.client.isOpen) {
      await this.client.connect();
    }
  }

  async disconnect() {
    await this.client.quit();
  }

  // Internal: compute score such that lower score = higher priority, earlier time
  static computeScore(priority, nowMs) {
    const numericPriority = Number.isFinite(priority) ? Number(priority) : 5;
    const safePriority = Math.max(0, Math.min(1000, Math.floor(numericPriority)));
    const PRIORITY_SCALE = 1e12; // keeps timestamp significance while grouping by priority
    return safePriority * PRIORITY_SCALE + nowMs;
  }

  taskKey(taskId) {
    return `${this.namespace}:task:${taskId}`;
  }

  // Public API: enqueue a task with payload and optional priority (0 highest)
  async enqueueTask(payload, priority = 5) {
    await this.connect();
    const taskId = randomUUID();
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
    // Expire task hash to avoid clutter if something goes wrong and it's never consumed (7 days)
    multi.expire(taskHashKey, 7 * 24 * 60 * 60);

    await multi.exec();
    return taskId;
  }

  // Distributed lock helpers
  async acquireLock() {
    const lockValue = randomUUID();
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
    if (!this.currentLockValue) return false;
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
    return result === 1;
  }

  async releaseLock() {
    if (!this.currentLockValue) return false;
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
    const released = result === 1;
    this.currentLockValue = null;
    return released;
  }

  async sleep(ms) {
    await new Promise((resolve) => setTimeout(resolve, ms));
  }

  // Start worker loop; handler receives { id, payload, priority, createdAtMs }
  async startWorker(handler, options = {}) {
    await this.connect();

    const {
      maxAttempts = 3,
      renewIntervalMs = Math.max(1000, Math.floor(this.lockTtlMs / 3)),
      batchSize = 1,
    } = options;

    this.workerAbort = false;

    const renewer = async () => {
      while (!this.workerAbort) {
        if (this.currentLockValue) {
          try {
            await this.renewLock();
          } catch (err) {
            this.log.warn('[lock] renew failed', err);
          }
        }
        await this.sleep(renewIntervalMs);
      }
    };

    // fire-and-forget renewal loop
    renewer();

    while (!this.workerAbort) {
      // Acquire the distributed lock
      let hasLock = false;
      try {
        hasLock = await this.acquireLock();
      } catch (err) {
        this.log.error('[lock] acquire error', err);
      }

      if (!hasLock) {
        await this.sleep(this.idleSleepMs);
        continue;
      }

      try {
        // Process up to batchSize tasks while holding the lock
        for (let i = 0; i < batchSize; i++) {
          const popped = await this.client.zPopMin(this.keys.pending);
          if (!popped || popped.length === 0) {
            break; // no tasks
          }

          const taskId = popped[0].value || popped[0];
          const taskKey = this.taskKey(taskId);
          const taskData = await this.client.hGetAll(taskKey);
          if (!taskData || !taskData.id) {
            continue; // task metadata missing, skip
          }

          const deserialized = {
            id: taskData.id,
            payload: this.safeParse(taskData.payload),
            priority: Number(taskData.priority || 5),
            createdAtMs: Number(taskData.createdAtMs || Date.now()),
            attempts: Number(taskData.attempts || 0),
          };

          // mark processing just for visibility
          await this.client.hSet(this.keys.processing, taskId, String(Date.now()));

          try {
            await handler(deserialized);
            // success cleanup
            const multi = this.client.multi();
            multi.hDel(this.keys.processing, taskId);
            multi.del(taskKey);
            await multi.exec();
          } catch (err) {
            // failure: increment attempts
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
              // requeue with same priority and original createdAt to preserve ordering
              const score = PriorityLockQueue.computeScore(deserialized.priority, deserialized.createdAtMs);
              const multi = this.client.multi();
              multi.lPush(this.keys.failed, failureRecord);
              multi.zAdd(this.keys.pending, [{ score, value: deserialized.id }]);
              multi.hDel(this.keys.processing, taskId);
              await multi.exec();
            } else {
              // move to failed queue only, do not requeue
              const multi = this.client.multi();
              multi.lPush(this.keys.failed, failureRecord);
              multi.hDel(this.keys.processing, taskId);
              // keep task hash for some time (already has expiry)
              await multi.exec();
            }
          }
        }
      } finally {
        await this.releaseLock();
      }

      // small pause to avoid hot loop when queue is empty
      await this.sleep(this.idleSleepMs);
    }
  }

  stopWorker() {
    this.workerAbort = true;
  }

  safeParse(str) {
    if (typeof str !== 'string') return str;
    try { return JSON.parse(str); } catch { return str; }
  }

  serializeError(err) {
    if (!err) return { message: 'Unknown error' };
    if (err instanceof Error) {
      return { name: err.name, message: err.message, stack: err.stack };
    }
    if (typeof err === 'object') return err;
    return { message: String(err) };
  }
}

module.exports = {
  PriorityLockQueue,
};