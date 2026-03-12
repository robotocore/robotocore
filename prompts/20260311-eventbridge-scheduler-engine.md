---
role: assistant
timestamp: "2026-03-11T21:30:00Z"
session: eventbridge-scheduler-engine
sequence: 1
---

# EventBridge Schedule Execution Engine

## What was built

Two background scheduler engines that make EventBridge scheduled rules and EventBridge Scheduler schedules actually fire their targets on cron/rate expressions.

### 1. EventBridge Rule Scheduler (`src/robotocore/services/events/rule_scheduler.py`)

- `EventBridgeRuleScheduler` singleton with daemon thread (5-second check interval)
- Iterates all EventsStore instances, finds rules with `schedule_expression` and `state == "ENABLED"`
- Parses `rate()` and `cron()` expressions using existing parsers from synthetics scheduler
- Tracks `_last_fired` timestamps to prevent double-fires within an interval
- Fires rules by calling `_dispatch_to_targets()` with a synthetic "Scheduled Event"

### 2. EventBridge Scheduler Executor (added to `src/robotocore/services/scheduler/provider.py`)

- `ScheduleExecutor` singleton with daemon thread (5-second check interval)
- Iterates all schedules in the global `_schedules` dict
- Dispatches to Lambda (via `invoke_lambda_async`), SQS (via Moto backend `send_message`), or SNS (via Moto backend `publish`)
- Uses `Target.Input` if set, otherwise generates a standard scheduled event JSON payload

### 3. Wired up in `src/robotocore/gateway/app.py`

Both engines start in `_start_background_engines()` alongside existing alarm scheduler, canary scheduler, etc.

## Design decisions

- **Reused synthetics parsers**: `parse_rate_seconds` and `parse_cron_minutes` from `robotocore.services.synthetics.scheduler` already handle the AWS expression formats. No reason to duplicate.
- **Followed alarm scheduler pattern exactly**: Singleton with lock, daemon thread, try/except in loop, monotonic time tracking. Proven pattern in this codebase.
- **Lazy imports for `_dispatch_to_targets` and `_stores`**: Avoids circular import issues since rule_scheduler imports from provider which has complex dependencies.

## Tests

- 28 unit tests covering parser functions, fire/no-fire logic, interval tracking, start/stop, and all three target types (Lambda, SQS, SNS) for the scheduler executor.
