---
session: "aec88107"
timestamp: "2026-03-12T07:01:00Z"
model: claude-opus-4-6
---

## Human

Fix four issues in the Lambda DLQ retry implementation in `src/robotocore/services/lambda_/provider.py`:
1. Retries are synchronous and block the 202 response
2. No exponential backoff between retries
3. MaximumEventAgeInSeconds not checked
4. Recursion should be replaced with iterative loop

## Assistant

## Key decisions

**Issue 1 — async dispatch**: Changed `_invoke()` to spawn `_dispatch_async_result()` in a
daemon `threading.Thread` instead of calling it synchronously. This matches AWS behavior where
the 202 response returns immediately and retries happen in the background.

**Issue 2 — exponential backoff**: Added `time.sleep(min(2 ** retry_attempt, 30))` before
each retry. Cap at 30 seconds to prevent excessive waits. Uses 1s, 2s, 4s progression.

**Issue 3 — MaximumEventAgeInSeconds**: Added `_invocation_time` parameter (defaults to
`time.time()`) and checks elapsed time against `MaximumEventAgeInSeconds` (default 6 hours)
before each retry. If exceeded, skips remaining retries and falls through to DLQ/OnFailure.

**Issue 4 — iterative loop**: Replaced recursive `_dispatch_async_result` calls with a
`while retry_attempt < max_retries` loop. This prevents stack overflow with large
MaximumRetryAttempts values and makes the control flow clearer.

All four fixes applied to a single rewrite of `_dispatch_async_result()` and its call site
in `_invoke()`. The `threading` and `time` imports were already present at the top of the file.
