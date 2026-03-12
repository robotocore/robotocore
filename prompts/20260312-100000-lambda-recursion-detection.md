---
session: "agent-aa06a690"
timestamp: "2026-03-12T10:00:00Z"
model: claude-opus-4-6
---

## Human

Implement Lambda recursion detection enforcement. The recursion config is stored via PutFunctionRecursionConfig / GetFunctionRecursionConfig but never enforced during invocations. When RecursiveLoop is "Terminate" (default), detect recursive invocations and terminate after threshold (16).

## Assistant

## Key decisions

**Separate module for recursion tracking**: Created `src/robotocore/services/lambda_/recursion.py` as a standalone module rather than inlining into provider.py. This keeps the recursion logic testable in isolation and avoids bloating the already-large provider module.

**Thread-safe depth counters**: Used a dict keyed by (account_id, region, func_name) with a threading lock, matching the pattern used elsewhere in the provider (e.g., _recursion_configs, _provisioned_concurrency). Each invocation increments on entry and decrements in a finally block.

**Two enforcement points**: Recursion detection is checked in both `_invoke()` (direct API invocations) and `_invoke_lambda_sync()` in invoke.py (cross-service invocations via SNS/SQS/EventBridge). Both paths needed coverage because a recursive loop can go through either path.

**Threshold of 16**: Matches AWS's documented behavior for recursive loop detection.

**check-then-increment pattern**: The check happens before increment so that depth 16 is the trigger point (0-15 are allowed, 16 triggers termination). The increment happens before execution so that nested calls during execution see the correct depth.

**N818 lint suppression**: The exception class is named `RecursiveInvocationException` (not `Error` suffix) to match the AWS API error code exactly. Added noqa comment explaining why.
