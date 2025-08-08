import type { RedisClientType } from 'redis';
import { v4 as uuidv4 } from 'uuid';

type ConsoleLike = {
  error: (...args: any[]) => void;
  warn: (...args: any[]) => void;
  log: (...args: any[]) => void;
};

export interface PriorityLockQueueOptions {
  redisClient: RedisClientType<any, any, any>;
  namespace?: string;
  lockTtlMs?: number;
  idleSleepMs?: number;
  log?: ConsoleLike;
}

export interface EnqueuedTaskMeta {
  id: string;
  payload: unknown;
  priority: number;
  createdAtMs: number;
  attempts: number;
}

export class PriorityLockQueue {
  private readonly client: RedisClientType<any, any, any>;
  private readonly namespace: string;
  private readonly keys: { pending: string; failed: string; processing: string; lock: string };
  private readonly lockTtlMs: number;
  private readonly idleSleepMs: number;
  private readonly log: ConsoleLike;

  private workerAbort: boolean;
  private currentLockValue: string | null;
  private didConnectInternally: boolean;

  constructor(options: PriorityLockQueueOptions) {
    const {
      redisClient,
      namespace = 'queue',
      lockTtlMs = 30000,
      idleSleepMs = 500,
      log = console,
    } = options;

    this.client = redisClient;
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

    const clientAsAny = this.client as any;
    if (clientAsAny && typeof clientAsAny.on === 'function') {
      clientAsAny.on('error', (err: unknown) => this.log.error('[redis] client error', err));
    }

    this.workerAbort = false;
    this.currentLockValue = null;
    this.didConnectInternally = false;
  }

  async connect(): Promise<void> {
    if (!this.client.isOpen) {
      await this.client.connect();
      this.didConnectInternally = true;
    }
  }

  async disconnect(): Promise<void> {
    if (this.didConnectInternally) {
      await this.client.quit();
      this.didConnectInternally = false;
    }
  }

  static computeScore(priority: number, nowMs: number): number {
    const numericPriority = Number.isFinite(priority) ? Number(priority) : 5;
    const safePriority = Math.max(0, Math.min(1000, Math.floor(numericPriority)));
    const PRIORITY_SCALE = 1e12;
    return safePriority * PRIORITY_SCALE + nowMs;
  }

  private taskKey(taskId: string): string {
    return `${this.namespace}:task:${taskId}`;
  }

  async enqueueTask(payload: unknown, priority: number = 5): Promise<string> {
    await this.connect();
    const taskId = uuidv4();
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
    } as any);
    multi.zAdd(this.keys.pending, [{ score, value: taskId }]);
    multi.expire(taskHashKey, 7 * 24 * 60 * 60);

    await multi.exec();
    return taskId;
  }

  private async acquireLock(): Promise<boolean> {
    const lockValue = uuidv4();
    const ok = await this.client.set(this.keys.lock, lockValue, {
      NX: true,
      PX: this.lockTtlMs,
    } as any);
    if (ok) {
      this.currentLockValue = lockValue;
      return true;
    }
    return false;
  }

  private async renewLock(): Promise<boolean> {
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
    } as any);
    return Number(result) === 1;
  }

  private async releaseLock(): Promise<boolean> {
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
    } as any);
    const released = Number(result) === 1;
    this.currentLockValue = null;
    return released;
  }

  private async sleep(ms: number): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, ms));
  }

  async startWorker(
    handler: (task: EnqueuedTaskMeta) => Promise<void>,
    options: { maxAttempts?: number; renewIntervalMs?: number; batchSize?: number } = {}
  ): Promise<void> {
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

    void renewer();

    while (!this.workerAbort) {
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
        for (let i = 0; i < batchSize; i++) {
          const popped = (await this.client.zPopMin(this.keys.pending)) as any;
          if (!popped || popped.length === 0) {
            break;
          }

          const taskId = (popped[0].value ?? popped[0]) as string;
          const taskKey = this.taskKey(taskId);
          const taskData = await this.client.hGetAll(taskKey);
          if (!taskData || !(taskData as any).id) {
            continue;
          }

          const deserialized: EnqueuedTaskMeta = {
            id: (taskData as any).id,
            payload: this.safeParse((taskData as any).payload),
            priority: Number((taskData as any).priority || 5),
            createdAtMs: Number((taskData as any).createdAtMs || Date.now()),
            attempts: Number((taskData as any).attempts || 0),
          };

          await this.client.hSet(this.keys.processing, taskId, String(Date.now()));

          try {
            await handler(deserialized);
            const multi = this.client.multi();
            multi.hDel(this.keys.processing, taskId);
            multi.del(taskKey);
            await multi.exec();
          } catch (err) {
            deserialized.attempts += 1;
            await this.client.hSet(taskKey, 'attempts', String(deserialized.attempts));

            const failureRecord = JSON.stringify({
              id: deserialized.id,
              payload: deserialized.payload,
              priority: deserialized.priority,
              createdAtMs: deserialized.createdAtMs,
              failedAtMs: Date.now(),
              attempts: deserialized.attempts,
              error: this.serializeError(err as any),
            });

            if (deserialized.attempts < maxAttempts) {
              const score = PriorityLockQueue.computeScore(deserialized.priority, deserialized.createdAtMs);
              const multi = this.client.multi();
              multi.lPush(this.keys.failed, failureRecord);
              multi.zAdd(this.keys.pending, [{ score, value: deserialized.id }]);
              multi.hDel(this.keys.processing, taskId);
              await multi.exec();
            } else {
              const multi = this.client.multi();
              multi.lPush(this.keys.failed, failureRecord);
              multi.hDel(this.keys.processing, taskId);
              await multi.exec();
            }
          }
        }
      } finally {
        await this.releaseLock();
      }

      await this.sleep(this.idleSleepMs);
    }
  }

  stopWorker(): void {
    this.workerAbort = true;
  }

  private safeParse(str: unknown): unknown {
    if (typeof str !== 'string') return str;
    try {
      return JSON.parse(str);
    } catch {
      return str;
    }
  }

  private serializeError(err: unknown): Record<string, unknown> {
    if (!err) return { message: 'Unknown error' };
    if (err instanceof Error) {
      return { name: err.name, message: err.message, stack: err.stack };
    }
    if (typeof err === 'object') return err as Record<string, unknown>;
    return { message: String(err) };
  }
}