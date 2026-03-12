---
session: "agent-a7393202"
timestamp: "2026-03-12T07:00:00Z"
model: claude-opus-4-6
---

## Human

Fix the async deadlock in Step Functions provider, same pattern as the Lambda fix in PR #124. `handle_stepfunctions_request()` is async but calls sync handler functions that call `executor.execute()` which can block. Also fix state manager save/load blocking in `app.py`.

## Assistant

## Key decisions

**Problem**: `handle_stepfunctions_request()` dispatches to sync handlers on the event loop. `_start_execution()` spawns a background thread (so it returns fast), but `_start_sync_execution()` calls `executor.execute()` inline and blocks the event loop entirely. Both need `asyncio.to_thread()` for safety: `_start_execution()` does lock acquisition and thread setup that could contend under load, and `_start_sync_execution()` is fully blocking.

**Fix pattern**: Wrapped `StartExecution` and `StartSyncExecution` handler calls in `asyncio.to_thread()`, matching the Lambda provider pattern. CRUD operations (create, describe, list, etc.) are fast in-memory lookups and don't need threading.

**State manager fix**: `save_state()`, `load_state()`, `export_state()`, and `import_state()` in `app.py` are async but call blocking pickle/tarfile/filesystem operations. Wrapped each `manager.*` call in `asyncio.to_thread()`.

**Tests**: Three compat tests verify the fix:
1. `test_start_execution_does_not_block_server` - starts a slow execution, verifies list_state_machines responds in <1s
2. `test_concurrent_executions_do_not_block_each_other` - fires 5 concurrent executions
3. `test_server_responsive_during_execution` - starts a DynamoDB Task execution, verifies server responds during it
