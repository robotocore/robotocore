---
session: "dev-ux-conformance"
timestamp: "2026-10-10T08:00:00Z"
model: claude-opus-4-6
sequence: 9
---

## Human

Continue improving robotocore (same standing goal).

## Assistant

## Key decisions

**SQS long-polls matched to a dedicated pool.** `ReceiveMessage` with
`WaitTimeSeconds>0` blocks in a thread — previously `asyncio.to_thread`, i.e.
the default executor that every other service shares with a small fixed worker
count. Dozens of concurrent long polls (a normal workload for a consumer
group) pin the pool and queue other services' calls behind them. The provider
now runs the wait on a bounded pool named `sqs-longpoll`
(`SQS_LONGPOLL_THREADS`, default 16), which keeps waits out of the shared pool
entirely: other `to_thread` calls can never be queued behind a long poll.

**Test**: inside the in-process app, a signed ReceiveMessage with
WaitTimeSeconds=1 must run on a pool-owned thread — the fixture patches
`StandardQueue.receive` and `FifoQueue.receive` (the two implementations that
can block) to record their thread name and asserts it starts with
`sqs-longpoll`; verified as a discriminator by reverting the provider to
`asyncio.to_thread` and watching it fail on the default pool's name.
