import { EventEmitter } from 'events';
import type { RedisClientType } from 'redis';
import { type EnqueuedTaskMeta } from './queue';
export interface QueueWorkerManagerOptions {
    redisClient: RedisClientType<any, any, any>;
    /** 默认 worker 配置：maxAttempts/batchSize/concurrency/renewIntervalMs */
    workerOptions?: {
        maxAttempts?: number;
        batchSize?: number;
        concurrency?: number;
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
export type ControlMessage = {
    action: 'start';
    namespace: string;
} | {
    action: 'stop';
    namespace: string;
} | {
    action: 'rescan';
};
/**
 * 单例：自动发现命名空间并为每个命名空间启动 worker；
 * 通过 Redis Pub/Sub 监听事件以按需启动/停止；避免重复监听和重复启动。
 */
export declare class QueueWorkerManager extends EventEmitter {
    private static instance;
    static getInstance(): QueueWorkerManager;
    private redisClient;
    private subscriber;
    private didCreateSubscriberInternally;
    private readonly runningByNamespace;
    private options;
    private scanTimer;
    private isStarted;
    private constructor();
    /**
     * 启动管理器：绑定 Redis、事件订阅与自动扫描
     */
    start(handler: (task: EnqueuedTaskMeta) => Promise<void>, options: QueueWorkerManagerOptions): Promise<void>;
    /**
     * 停止所有 worker，并取消订阅。
     */
    stop(): Promise<void>;
    /**
     * 处理控制消息
     */
    private handleControlMessage;
    /**
     * 启动指定命名空间的 worker（若未运行）。
     */
    startNamespace(namespace: string, handler: (task: EnqueuedTaskMeta) => Promise<void>): Promise<void>;
    /**
     * 停止指定命名空间的 worker（若在运行）。
     */
    stopNamespace(namespace: string): Promise<void>;
    /**
     * 自动发现命名空间并为未运行的命名空间启动 worker。
     */
    discoverAndStartAll(handler: (task: EnqueuedTaskMeta) => Promise<void>): Promise<void>;
    /**
     * 从 Redis 扫描命名空间（基于 *:pending）。
     */
    discoverNamespaces(): Promise<string[]>;
    /**
     * 确保有 subscriber 连接用于订阅控制通道。
     */
    private ensureSubscriber;
}
