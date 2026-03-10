---
session: "a3045cbb"
timestamp: "2026-03-10T15:39:16Z"
model: claude-opus-4-6
---

## Human

Implement StepFunctions async execution in robotocore. Currently StartExecution blocks synchronously; there's no background execution for Standard workflows. StartExecution should return immediately with RUNNING status, a background thread should execute the state machine, and StopExecution should signal abort. StartSyncExecution should remain synchronous for EXPRESS workflows.

## Assistant

## Key decisions

**Background thread per execution**: Each StartExecution spawns a daemon thread that runs the ASL executor. The execution is stored immediately with RUNNING status before the thread starts, so DescribeExecution always returns a valid result. On completion the thread updates the status to SUCCEEDED or FAILED under the lock.

**Abort signaling via threading.Event**: Each running execution gets an associated Event in `_abort_events`. StopExecution sets this event and immediately marks the execution as ABORTED. The background thread checks the abort event in its finally block for cleanup. This is a cooperative abort -- the ASL executor itself doesn't check for abort mid-state, but the status is set immediately so DescribeExecution reflects the abort.

**No change to StartSyncExecution**: EXPRESS workflows continue to execute synchronously and return the full result inline, matching AWS behavior.

**Thread cleanup**: Background threads are daemon threads (won't block process exit) and are tracked in `_running_threads` dict. The finally block in the background worker removes the thread and abort event from the registries.

**StopExecution preserves error/cause**: Added support for the optional `error` and `cause` parameters on StopExecution, matching the AWS API.
