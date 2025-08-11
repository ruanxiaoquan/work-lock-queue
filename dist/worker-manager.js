"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.QueueWorkerManager = void 0;
const events_1 = require("events");
const queue_1 = require("./queue");
/**
 * 单例：自动发现命名空间并为每个命名空间启动 worker；
 * 通过 Redis Pub/Sub 监听事件以按需启动/停止；避免重复监听和重复启动。
 */
class QueueWorkerManager extends events_1.EventEmitter {
    static getInstance() {
        if (!QueueWorkerManager.instance) {
            QueueWorkerManager.instance = new QueueWorkerManager();
        }
        return QueueWorkerManager.instance;
    }
    constructor() {
        super();
        this.redisClient = null;
        this.subscriber = null;
        this.didCreateSubscriberInternally = false;
        this.runningByNamespace = new Map();
        this.options = {
            redisClient: null,
            workerOptions: {},
            queueOptions: {},
            scanIntervalMs: 0,
            controlChannel: 'queue:worker:control',
            log: console,
            closeInternalSubscriberOnStop: true,
        };
        this.scanTimer = null;
        this.isStarted = false;
    }
    /**
     * 启动管理器：绑定 Redis、事件订阅与自动扫描
     */
    async start(handler, options) {
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
            closeInternalSubscriberOnStop: options.closeInternalSubscriberOnStop ?? true,
        };
        this.redisClient = options.redisClient;
        // 初次扫描并启动
        await this.discoverAndStartAll(handler);
        // 定时扫描
        if (options.scanIntervalMs && options.scanIntervalMs > 0) {
            this.scanTimer = setInterval(() => {
                this.discoverAndStartAll(handler).catch((err) => this.options.log?.error('[manager] periodic scan error', err));
            }, options.scanIntervalMs);
        }
        // 订阅控制通道
        await this.ensureSubscriber();
        await this.subscriber.subscribe(this.options.controlChannel, async (raw) => {
            try {
                const msg = JSON.parse(raw);
                await this.handleControlMessage(msg, handler);
            }
            catch (err) {
                this.options.log?.warn('[manager] invalid control message', raw, err);
            }
        });
    }
    /**
     * 停止所有 worker，并取消订阅。
     */
    async stop() {
        if (!this.isStarted)
            return;
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
            }
            catch (err) {
                this.options.log?.warn(`[manager] stop worker failed for ${ns}`, err);
            }
        }
        this.runningByNamespace.clear();
        // 取消订阅并关闭 subscriber（若内部创建）
        if (this.subscriber) {
            try {
                await this.subscriber.unsubscribe(this.options.controlChannel);
            }
            catch { }
            if (this.didCreateSubscriberInternally && this.options.closeInternalSubscriberOnStop) {
                try {
                    await this.subscriber.quit();
                }
                catch { }
            }
        }
        this.subscriber = null;
        this.didCreateSubscriberInternally = false;
    }
    /**
     * 处理控制消息
     */
    async handleControlMessage(msg, handler) {
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
    async startNamespace(namespace, handler) {
        if (this.runningByNamespace.has(namespace))
            return;
        const queue = new queue_1.PriorityLockQueue({
            redisClient: this.redisClient,
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
        const baseWorkerOptions = { ...(this.options.workerOptions || {}) };
        const cw = baseWorkerOptions.concurrency;
        let effectiveConcurrency;
        if (cw && typeof cw === 'object') {
            const mapped = cw[namespace];
            if (Number.isFinite(mapped)) {
                effectiveConcurrency = Math.max(1, Math.floor(mapped));
            }
            else {
                effectiveConcurrency = 5; // 默认并发
            }
        }
        else {
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
    async stopNamespace(namespace) {
        const running = this.runningByNamespace.get(namespace);
        if (!running)
            return;
        try {
            running.queue.stopWorker();
            running.stopped = true;
        }
        finally {
            this.runningByNamespace.delete(namespace);
        }
    }
    /**
     * 自动发现命名空间并为未运行的命名空间启动 worker。
     */
    async discoverAndStartAll(handler) {
        if (!this.redisClient)
            throw new Error('QueueWorkerManager not initialized: redisClient missing');
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
    async discoverNamespaces() {
        if (!this.redisClient)
            throw new Error('QueueWorkerManager not initialized: redisClient missing');
        const discovered = new Set();
        const addFromPattern = async (pattern) => {
            for await (const key of this.redisClient.scanIterator({ MATCH: pattern, COUNT: 200 })) {
                const k = String(key);
                if (k.endsWith(':pending'))
                    discovered.add(k.replace(/:pending$/, ''));
                else if (k.endsWith(':processing'))
                    discovered.add(k.replace(/:processing$/, ''));
                else if (k.endsWith(':failed'))
                    discovered.add(k.replace(/:failed$/, ''));
                else if (k.endsWith(':succeeded'))
                    discovered.add(k.replace(/:succeeded$/, ''));
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
    async ensureSubscriber() {
        if (this.subscriber && this.subscriber.isOpen)
            return;
        if (!this.redisClient)
            throw new Error('QueueWorkerManager not initialized: redisClient missing');
        // 使用 duplicate 创建订阅连接
        this.subscriber = this.redisClient.duplicate();
        this.didCreateSubscriberInternally = true;
        const anySub = this.subscriber;
        if (!anySub.isOpen) {
            await this.subscriber.connect();
        }
        // 错误日志
        if (typeof anySub.on === 'function') {
            anySub.on('error', (err) => this.options.log?.error('[redis:subscriber] error', err));
        }
    }
}
exports.QueueWorkerManager = QueueWorkerManager;
QueueWorkerManager.instance = null;
//# sourceMappingURL=worker-manager.js.map