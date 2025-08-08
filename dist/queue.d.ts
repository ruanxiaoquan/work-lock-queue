import type { RedisClientType } from 'redis';
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
export declare class PriorityLockQueue {
    private readonly client;
    private readonly namespace;
    private readonly keys;
    private readonly lockTtlMs;
    private readonly idleSleepMs;
    private readonly log;
    private workerAbort;
    private currentLockValue;
    private didConnectInternally;
    constructor(options: PriorityLockQueueOptions);
    connect(): Promise<void>;
    disconnect(): Promise<void>;
    static computeScore(priority: number, nowMs: number): number;
    private taskKey;
    enqueueTask(payload: unknown, priority?: number): Promise<string>;
    private acquireLock;
    private renewLock;
    private releaseLock;
    private sleep;
    startWorker(handler: (task: EnqueuedTaskMeta) => Promise<void>, options?: {
        maxAttempts?: number;
        renewIntervalMs?: number;
        batchSize?: number;
    }): Promise<void>;
    stopWorker(): void;
    private safeParse;
    private serializeError;
}
export {};
