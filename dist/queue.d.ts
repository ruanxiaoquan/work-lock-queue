import type { RedisClientType } from 'redis';
/**
 * 任务优先级枚举（数字越小，优先级越高）。
 * 建议范围：0（最高）~ 1000（最低）。
 */
export declare enum TaskPriority {
    /** 默认优先级（1），高于 MEDIUM/HIGH 的语义仅作示例，数字越小越先处理 */
    DEFAULT = 1,
    /** 中等优先级（2） */
    MEDIUM = 2,
    /** 较高优先级（3），注意数值越小优先级越高，0 最高 */
    HIGH = 3
}
/**
 * 队列构造参数。
 */
export interface PriorityLockQueueOptions {
    /** Redis 客户端实例（已由外部创建/管理） */
    redisClient: RedisClientType<any, any, any>;
    /** 命名空间，用作所有 Redis key 的前缀，默认 'queue' */
    namespace?: string;
    /** 分布式锁 TTL（毫秒），默认 30000ms。必须大于续约间隔 */
    lockTtlMs?: number;
    /** 空转等待时长（毫秒），用于无锁或无任务时的短暂休眠，默认 500ms */
    idleSleepMs?: number;
    /** 日志对象，默认 console */
    log?: Console;
}
/**
 * 入队任务的元数据（存储在 Redis HASH 中）。
 */
export interface EnqueuedTaskMeta {
    /** 任务 ID（UUID） */
    id: string;
    /** 任务载荷（用户自定义，JSON 序列化存储） */
    payload: unknown;
    /** 优先级（数字越小优先级越高） */
    priority: number;
    /** 入队时间戳（毫秒） */
    createdAtMs: number;
    /** 已尝试执行次数 */
    attempts: number;
}
/**
 * 基于 Redis 的带优先级、分布式锁的队列。
 * - 仅持有锁的 worker 才能批量拉取并处理任务；
 * - 优先级越高（数字越小）越先被处理；
 * - 失败任务会被记录到失败列表，并按配置重试；
 * - 提供基本指标与观测支持（processing/failed/succeeded）。
 */
export declare class PriorityLockQueue {
    /** Redis 客户端 */
    private readonly client;
    /** 队列命名空间（Redis key 前缀） */
    private readonly namespace;
    /** 各类 Redis key */
    private readonly keys;
    /** 锁的 TTL（毫秒） */
    private readonly lockTtlMs;
    /** 空闲睡眠时长（毫秒） */
    private readonly idleSleepMs;
    /** 日志对象 */
    private readonly log;
    /** 标记是否请求停止 worker 循环 */
    private workerAbort;
    /** 当前持有锁时保存的随机值（用于续约/释放校验） */
    private currentLockValue;
    /** 标记 connect() 是否在内部调用（用于决定 disconnect 行为） */
    private didConnectInternally;
    /**
     * 构造函数。
     * @param options 配置项（见 PriorityLockQueueOptions）
     */
    constructor(options: PriorityLockQueueOptions);
    /**
     * 确保 Redis 已连接。
     * - 如果外部尚未连接，则在此连接，并记录内部连接标记
     */
    connect(): Promise<void>;
    /**
     * 关闭 Redis 连接（仅当 connect() 是内部打开时）。
     */
    disconnect(): Promise<void>;
    /**
     * 计算任务的排序分数（score）。
     * 分数 = priority * 1e12 + nowMs，数值越小越先被处理。
     */
    static computeScore(priority: number, nowMs: number): number;
    /**
     * 获取任务在 Redis 中的 HASH key。
     */
    private taskKey;
    /** 暴露当前命名空间 */
    getNamespace(): string;
    /**
     * 获取队列当前指标（pending/processing/failed 数量）。
     */
    getMetrics(): Promise<{
        namespace: string;
        pendingCount: number;
        processingCount: number;
        failedCount: number;
        succeededCount: number;
    }>;
    /**
     * 入队一个任务。
     * @param payload 任务载荷（对象将被 JSON 序列化存储）
     * @param priority 优先级（数字越小越高），默认 TaskPriority.DEFAULT
     * @returns 任务 ID
     */
    enqueueTask(payload: unknown, priority: TaskPriority.DEFAULT): Promise<string>;
    /**
     * 尝试获取分布式锁（SET NX PX）。
     * 成功返回 true，并记录当前锁值用于续约/释放。
     */
    private acquireLock;
    /**
     * 续约分布式锁（Lua 校验锁值一致后 pexpire）。
     */
    private renewLock;
    /**
     * 释放分布式锁（Lua 校验锁值一致后 del）。
     */
    private releaseLock;
    /**
     * Promise 版 sleep，worker 空转时使用以避免热循环。
     */
    private sleep;
    /**
     * 启动 worker 主循环。
     * - 仅持有锁的实例会拉取任务；
     * - 每轮最多拉取 batchSize 个任务，且最多并发 concurrency 个执行；
     * - 失败按 maxAttempts 控制是否重试，失败记录写入失败列表；
     * - 期间后台协程按 renewIntervalMs 定期续约锁。
     */
    startWorker(handler: (task: EnqueuedTaskMeta) => Promise<void>, options?: {
        /** 最大重试次数（不含首次执行），默认 3 */
        maxAttempts?: number;
        /** 锁续约间隔（毫秒），默认 max(1000, lockTtlMs/3) */
        renewIntervalMs?: number;
        /** 每轮最多拉取的任务数（应 >= concurrency 以充分并发），默认 1 */
        batchSize?: number;
        /** 单轮最大并发执行数，默认 1 */
        concurrency?: number;
    }): Promise<void>;
    /**
     * 请求停止 worker（安全停止：会在当前循环点生效）。
     */
    stopWorker(): void;
    /** 安全 JSON 解析：失败时返回原字符串 */
    private safeParse;
    /** 将错误对象序列化为可 JSON 化的结构 */
    private serializeError;
}
