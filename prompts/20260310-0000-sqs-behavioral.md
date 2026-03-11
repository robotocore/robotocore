---
role: assistant
timestamp: "2026-03-10T00:00:00Z"
session: "sqs-behavioral"
sequence: 1
---

## SQS Behavioral Fidelity Implementation

Implemented three AWS-matching behavioral features for the native SQS provider:

1. **PurgeQueueInProgress**: Tracks last purge time per queue. Returns 403 PurgeQueueInProgress if same queue is purged within 60 seconds. Configurable via `SQS_DELAY_PURGE_RETRY` env var.

2. **QueueDeletedRecently**: Tracks deleted queue names with timestamps. Returns 400 QueueDeletedRecently if a queue with the same name is created within 60 seconds of deletion. Configurable via `SQS_DELAY_RECENTLY_DELETED` env var.

3. **Message Retention Period**: Background thread scans queues every 60 seconds and removes messages older than the queue's `MessageRetentionPeriod` attribute (default 4 days). Configurable via `SQS_ENABLE_MESSAGE_RETENTION_PERIOD` and `SQS_RETENTION_SCAN_INTERVAL` env vars.

### Design decisions
- All three features are in a separate `behavioral.py` module to keep the provider clean
- Each tracker is thread-safe with its own lock
- The retention scanner uses `threading.Event.wait(timeout=...)` for clean shutdown
- All features default to enabled (matching AWS behavior) but can be disabled for tests that don't want the delays
- Exception types are specific (`PurgeQueueInProgressError`, `QueueDeletedRecentlyError`) so the provider handler can map them to correct AWS error codes

### Test approach
- 15 unit tests covering each feature in isolation (env var toggle, time mocking, queue isolation)
- 5 integration tests exercising the full provider path (action handlers + behavioral checks together)
- All 20 tests pass in ~0.03s
