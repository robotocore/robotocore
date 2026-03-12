---
session: "a43a4a24"
timestamp: "2026-03-12T09:00:00Z"
model: claude-opus-4-6
---

## Human

Write stress tests that verify robotocore handles concurrent requests correctly. Race conditions are the bugs users discover in production that are hardest to debug. Test scenarios: concurrent S3 puts, concurrent SQS sends+receives, concurrent DynamoDB writes, concurrent Lambda invocations, cross-service concurrent, rapid create/delete cycle, concurrent state save, concurrent API Gateway requests.

## Assistant

## Key decisions

**Test file**: `tests/compatibility/test_concurrent_requests_compat.py` with 10 tests across 8 test classes.

**Approach**: Used `concurrent.futures.ThreadPoolExecutor` for all concurrency. Each test creates resources with unique names (uuid-based) to avoid collisions between test runs. All tests have thorough cleanup in `finally` blocks.

**State save fix**: The `/_robotocore/state/save` endpoint requires either `ROBOTOCORE_STATE_DIR` env var or a `path` parameter in the request body. Used `tempfile.mkdtemp()` to provide a temporary path.

**Test quality**: 100% effective test rate (all 10 tests contact the server and assert on response fields). No ParamValidationError catches, no xfails.

**Concurrency levels**: 10 threads for S3/DynamoDB puts, 5+5 for SQS send/receive, 5 for Lambda invocations, 3 for cross-service, 2 for state save, 5 for API Gateway. These levels are high enough to surface race conditions but low enough to complete quickly (~6s total).
