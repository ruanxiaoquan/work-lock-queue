# Redis Priority Lock Queue (Node.js)

Redis-backed queue that ensures only one worker processes tasks at a time across instances via a preemptive distributed lock. Tasks are queued with priority and failures are sent to a failure list.

- Single producer API: `enqueueTask(payload, priority)` enqueues tasks to a run queue
- Worker processes tasks across the cluster using a Redis lock with configurable concurrency per worker
- On failure, tasks are logged to a failure queue and retried up to `maxAttempts`
- Priority: lower numeric priority is higher (0 highest)

## Install

```bash
npm install
```

Set `REDIS_URL` if not on localhost.

## Usage

Enqueue tasks:

```js
const { PriorityLockQueue } = require('./src');

(async () => {
  const queue = new PriorityLockQueue({ namespace: 'jobs', redisUrl: process.env.REDIS_URL });
  await queue.enqueueTask({ userId: 123 }, 0); // highest priority
  await queue.enqueueTask({ userId: 456 }, 5); // normal priority
})();
```

Run worker:

```bash
CONCURRENCY=5 npm run start:worker
```

Environment:

- `REDIS_URL` (default `redis://localhost:6379`)
- `QUEUE_NAMESPACE` (default `demo`)

## How it works

- Pending queue: `namespace:pending` (ZSET). Score = `priority * 1e12 + createdAtMs` so lower score pops first
- Task metadata: `namespace:task:{id}` (HASH)
- Failure queue: `namespace:failed` (LIST)
- Lock: `namespace:lock` with `SET NX PX` and safe renew/release using Lua

Worker loop:

1. Acquire lock
2. Pop up to `batchSize` tasks, running at most `concurrency` at a time
3. Run handler(s)
4. On success: delete task metadata
5. On failure: push record to `failed` list and optionally requeue until `maxAttempts`
6. Release lock after all started tasks finish

## Example

- Enqueue demo tasks: `npm run start:demo`
- Start worker: `npm run start:worker`

## Notes

- Priority: 0 is highest. Tasks within the same priority are FIFO by enqueue time
- Lock TTL is renewed in the background while processing
- If there are no tasks, the worker releases the lock and sleeps briefly to avoid hot looping

## Concurrency

`startWorker(handler, { concurrency, batchSize, maxAttempts })`

- `concurrency`: maximum number of tasks processed in parallel in one lock window (default 1)
- `batchSize`: upper bound of tasks pulled per iteration (default 1). It should be >= `concurrency` to fully utilize parallelism
- Example: `CONCURRENCY=10 npm run start:worker`