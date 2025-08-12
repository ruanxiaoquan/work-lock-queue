import { EventEmitter } from 'events';
import type { RedisClientType } from 'redis';
import { PriorityLockQueue, type EnqueuedTaskMeta } from './queue';

export interface QueueWorkerManagerOptions {
  redisClient: RedisClientType<any, any, any>;
  /** 默认 worker 配置：maxAttempts/batchSize/concurrency/renewIntervalMs */
  workerOptions?: {
    maxAttempts?: number;
    batchSize?: number;
    concurrency?: Record<string, number>;
    renewIntervalMs?: number;
  };
  /** 每个命名空间队列的 lockTtl/idlesleep 配置（传入 PriorityLockQueue） */
  queueOptions?: {
    lockTtlMs?: number;
    idleSleepMs?: number;
  };
  /** 自动扫描命名空间的间隔（毫秒）。0/undefined 则仅启动时扫描一次 */
  scanIntervalMs?: number;
  /** 控制通道名称（Redis Pub/Sub）。默认 queue:worker:control */
  controlChannel?: string;
  /** 日志对象，默认 console */
  log?: Console;
  /** 是否在 stop() 时关闭内部创建的 subscriber 连接（默认 true） */
  closeInternalSubscriberOnStop?: boolean;
}

export type ControlMessage =
  | { action: 'start'; namespace: string }
  | { action: 'stop'; namespace: string }
  | { action: 'rescan' };

interface RunningWorker {
  namespace: string;
  queue: PriorityLockQueue;
  /** 标记该 worker 是否处于停止请求状态 */
  stopped: boolean;
}

/**
 * 单例：自动发现命名空间并为每个命名空间启动 worker；
 * 通过 Redis Pub/Sub 监听事件以按需启动/停止；避免重复监听和重复启动。
 */
export class QueueWorkerManager extends EventEmitter {
  private static instance: QueueWorkerManager | null = null;

  public static getInstance(): QueueWorkerManager {
    if (!QueueWorkerManager.instance) {
      QueueWorkerManager.instance = new QueueWorkerManager();
    }
    return QueueWorkerManager.instance;
  }

  private redisClient: RedisClientType<any, any, any> | null = null;
  private subscriber: RedisClientType<any, any, any> | null = null;
  private didCreateSubscriberInternally = false;

  private readonly runningByNamespace: Map<string, RunningWorker> = new Map();
  private options: Required<Pick<QueueWorkerManagerOptions, 'controlChannel' | 'closeInternalSubscriberOnStop'>> &
    Omit<QueueWorkerManagerOptions, 'controlChannel' | 'closeInternalSubscriberOnStop'> = {
    redisClient: null as any,
    workerOptions: {},
    queueOptions: {},
    scanIntervalMs: 0,
    controlChannel: 'queue:worker:control',
    log: console,
    closeInternalSubscriberOnStop: true,
  } as any;

  private scanTimer: NodeJS.Timeout | null = null;
  private isStarted = false;

  private constructor() {
    super();
  }

  /**
   * 启动管理器：绑定 Redis、事件订阅与自动扫描
   */
  public async start(
    handler: (task: EnqueuedTaskMeta) => Promise<void>,
    options: QueueWorkerManagerOptions
  ): Promise<void> {
    if (this.isStarted) {
      this.options.log?.warn('[manager] already started; ignoring duplicate start()');
      return;
    }

    this.isStarted = true;
    // 合并配置
    this.options = {
      ...this.options,
      ...options,
      controlChannel: options.controlChannel || 'queue:worker:control',
      log: options.log || console,
      closeInternalSubscriberOnStop:
        options.closeInternalSubscriberOnStop ?? true,
    } as any;

    this.redisClient = options.redisClient;

    // 初次扫描并启动
    await this.discoverAndStartAll(handler);

    // 定时扫描
    if (options.scanIntervalMs && options.scanIntervalMs > 0) {
      this.scanTimer = setInterval(() => {
        this.discoverAndStartAll(handler).catch((err) =>
          this.options.log?.error('[manager] periodic scan error', err)
        );
      }, options.scanIntervalMs);
    }

    // 订阅控制通道
    await this.ensureSubscriber();
    await this.subscriber!.subscribe(this.options.controlChannel!, async (raw) => {
      try {
        const msg: ControlMessage = JSON.parse(raw);
        await this.handleControlMessage(msg, handler);
      } catch (err) {
        this.options.log?.warn('[manager] invalid control message', raw, err);
      }
    });
  }

  /**
   * 停止所有 worker，并取消订阅。
   */
  public async stop(): Promise<void> {
    if (!this.isStarted) return;
    this.isStarted = false;

    if (this.scanTimer) {
      clearInterval(this.scanTimer);
      this.scanTimer = null;
    }

    // 停止所有正在运行的 worker
    for (const [ns, running] of this.runningByNamespace) {
      try {
        running.queue.stopWorker();
        running.stopped = true;
      } catch (err) {
        this.options.log?.warn(`[manager] stop worker failed for ${ns}`, err);
      }
    }
    this.runningByNamespace.clear();

    // 取消订阅并关闭 subscriber（若内部创建）
    if (this.subscriber) {
      try {
        await this.subscriber.unsubscribe(this.options.controlChannel!);
      } catch {}

      if (this.didCreateSubscriberInternally && this.options.closeInternalSubscriberOnStop) {
        try {
          await this.subscriber.quit();
        } catch {}
      }
    }

    this.subscriber = null;
    this.didCreateSubscriberInternally = false;
  }

  /**
   * 处理控制消息
   */
  private async handleControlMessage(
    msg: ControlMessage,
    handler: (task: EnqueuedTaskMeta) => Promise<void>
  ): Promise<void> {
    if (msg.action === 'rescan') {
      await this.discoverAndStartAll(handler);
      return;
    }

    if (msg.action === 'start' && msg.namespace) {
      await this.startNamespace(msg.namespace, handler);
      return;
    }

    if (msg.action === 'stop' && msg.namespace) {
      await this.stopNamespace(msg.namespace);
      return;
    }
  }

  /**
   * 启动指定命名空间的 worker（若未运行）。
   */
  public async startNamespace(
    namespace: string,
    handler: (task: EnqueuedTaskMeta) => Promise<void>
  ): Promise<void> {
    if (this.runningByNamespace.has(namespace)) return;

    const queue = new PriorityLockQueue({
      redisClient: this.redisClient!,
      namespace,
      ...(this.options.queueOptions || {}),
      log: this.options.log || console,
    });

    this.runningByNamespace.set(namespace, {
      namespace,
      queue,
      stopped: false,
    });

    // 组装针对该命名空间的有效 worker 配置
    const baseWorkerOptions = { ...(this.options.workerOptions || {}) } as any;
    const cw = baseWorkerOptions.concurrency as Record<string, number> | undefined;
    let effectiveConcurrency: number;
    if (cw && typeof cw === 'object') {
      const mapped = cw[namespace];
      if (Number.isFinite(mapped as number)) {
        effectiveConcurrency = Math.max(1, Math.floor(mapped as number));
      } else {
        effectiveConcurrency = 5; // 默认并发
      }
    } else {
      effectiveConcurrency = 5; // 未配置则默认 5
    }
    baseWorkerOptions.concurrency = effectiveConcurrency;

    // 不等待主循环（避免阻塞）
    queue
      .startWorker(handler, baseWorkerOptions)
      .catch((err) => this.options.log?.error(`[worker:${namespace}] loop error`, err))
      .finally(() => {
        // 主循环退出后，从运行表中移除
        const running = this.runningByNamespace.get(namespace);
        if (running && running.queue === queue) {
          this.runningByNamespace.delete(namespace);
        }
      });
  }

  /**
   * 停止指定命名空间的 worker（若在运行）。
   */
  public async stopNamespace(namespace: string): Promise<void> {
    const running = this.runningByNamespace.get(namespace);
    if (!running) return;
    try {
      running.queue.stopWorker();
      running.stopped = true;
    } finally {
      this.runningByNamespace.delete(namespace);
    }
  }

  /**
   * 自动发现命名空间并为未运行的命名空间启动 worker。
   */
  public async discoverAndStartAll(
    handler: (task: EnqueuedTaskMeta) => Promise<void>
  ): Promise<void> {
    if (!this.redisClient) throw new Error('QueueWorkerManager not initialized: redisClient missing');

    const namespaces = await this.discoverNamespaces();
    for (const ns of namespaces) {
      if (!this.runningByNamespace.has(ns)) {
        await this.startNamespace(ns, handler);
      }
    }
  }

  /**
   * 从 Redis 扫描命名空间（基于 *:pending 以及 processing/failed/succeeded）。
   */
  public async discoverNamespaces(): Promise<string[]> {
    if (!this.redisClient) throw new Error('QueueWorkerManager not initialized: redisClient missing');

    const discovered = new Set<string>();

    const scanKeysWithPattern = async (pattern: string): Promise<string[]> => {
      const allKeys: string[] = [];
      let cursor = '0';
      
      do {
        const result = await this.redisClient!.scan(cursor, {
          MATCH: pattern,
          COUNT: 200
        });
        
        cursor = result.cursor;
        allKeys.push(...result.keys);
      } while (cursor !== '0');
      
      return allKeys;
    };

    const addFromPattern = async (pattern: string) => {
      const keys = await scanKeysWithPattern(pattern);
      for (const key of keys) {
        const k = String(key);
        if (k.endsWith(':pending')) discovered.add(k.replace(/:pending$/, ''));
        else if (k.endsWith(':processing')) discovered.add(k.replace(/:processing$/, ''));
        else if (k.endsWith(':failed')) discovered.add(k.replace(/:failed$/, ''));
        else if (k.endsWith(':succeeded')) discovered.add(k.replace(/:succeeded$/, ''));
      }
    };

    await Promise.all([
      addFromPattern('*:pending'),
      addFromPattern('*:processing'),
      addFromPattern('*:failed'),
      addFromPattern('*:succeeded'),
    ]);

    return Array.from(discovered);
  }

  /**
   * 确保有 subscriber 连接用于订阅控制通道。
   */
  private async ensureSubscriber(): Promise<void> {
    if (this.subscriber && (this.subscriber as any).isOpen) return;
    if (!this.redisClient) throw new Error('QueueWorkerManager not initialized: redisClient missing');

    // 使用 duplicate 创建订阅连接
    this.subscriber = this.redisClient.duplicate();
    this.didCreateSubscriberInternally = true;
    const anySub = this.subscriber as any;
    if (!anySub.isOpen) {
      await this.subscriber.connect();
    }

    // 错误日志
    if (typeof anySub.on === 'function') {
      anySub.on('error', (err: unknown) => this.options.log?.error('[redis:subscriber] error', err));
    }
  }
}