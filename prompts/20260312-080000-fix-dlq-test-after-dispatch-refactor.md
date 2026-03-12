---
session: "pr128-fix"
timestamp: "2026-03-12T08:00:00Z"
model: claude-opus-4-6
---

## Human

PR #128 has unit test failures in CI. The `unit: services` jobs fail on both Python 3.12 and 3.13. Fix the failures and merge main.

## Assistant

The CI failure was in `test_dispatch_to_dlq_sqs` in `tests/unit/services/test_lambda_features.py`. The provider was refactored to use `queue.put(SqsMessage(...))` instead of `queue.send_message(body=...)`, but the test still asserted on the old `send_message` mock call. Updated the test to assert on `queue.put` and extract the body from the `SqsMessage` object passed as the first positional arg. Also merged `origin/main` to pick up recent lambda-function-url-routing and stepfunctions-async-deadlock changes.
