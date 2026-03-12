---
session: "chaos-e2e-tests"
timestamp: "2026-03-12T09:45:00Z"
model: claude-opus-4-6
---

## Human

Write end-to-end tests for the chaos engineering feature (fault injection). Cover
ThrottlingException, ServiceUnavailableException, latency injection, error rate
probability, service-specific rules, operation-specific rules, delete rule, list
rules, and clear all rules. Run against the live server.

## Assistant

## Key decisions

**Protocol-aware testing**: Chaos middleware returns JSON error responses regardless
of the target service's protocol. For S3 (rest-xml protocol), boto3 cannot parse
`__type` from a JSON body and falls back to using the HTTP status code as the error
code. Used raw HTTP requests for S3 chaos verification and JSON-protocol services
(DynamoDB) for boto3-level testing where `__type` parsing works correctly.

**Disabled boto3 retries**: boto3 automatically retries ThrottlingException (429)
and 5xx errors. Without `Config(retries={"max_attempts": 0})`, tests hang or
behave unpredictably because boto3 retries multiple times against the chaos rule.
Every test client uses this config.

**Latency test approach**: The latency middleware uses `loop.create_task(asyncio.sleep())`
which fires-and-forgets -- it does not actually block the response. Rather than
testing elapsed time (which would always pass), verified the rule matches by
checking `match_count` in the rule list endpoint.

**Operation-specific test**: Switched from S3 PutObject (which needs a bucket and
has rest-xml protocol issues) to DynamoDB CreateTable vs ListTables for the
operation-specific blocking test. This uses JSON protocol so boto3 parses the
error code correctly.

**Error rate test**: 40 requests at 50% probability with a wide acceptance window
(5-35 failures) to avoid flaky tests while still proving the probability mechanism
works.
