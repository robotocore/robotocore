---
session: "a77204ad"
timestamp: "2026-03-12T06:00:00Z"
model: claude-opus-4-6
---

## Human

Write end-to-end tests for Lambda cross-service integrations. Tests should verify
Lambda works when calling other AWS services (S3, DynamoDB, SQS, SNS, etc.) and
when triggered by other services (API Gateway). Also test versioning, concurrency,
VPC config, and tags.

## Assistant

## Key decisions

**Deadlock fix**: Discovered that the Lambda in-process executor (`_invoke` in
`provider.py`) called `executor.execute()` synchronously within an `async` handler,
blocking the ASGI event loop. When Lambda code called back to the server (e.g.,
`s3.put_object(endpoint_url="http://localhost:4566")`), the server couldn't respond
because its event loop was blocked waiting for the Lambda to finish -- classic deadlock.

**Fix**: Wrapped both the local and Docker executor calls in `asyncio.to_thread()` so
the ASGI event loop stays free while Lambda code executes. This mirrors the pattern
already used in `invoke.py` (the cross-service invocation path) which uses a thread pool.

**Test design**: Each cross-service test creates a Lambda function whose handler code
uses boto3 with `endpoint_url` pointing back to the emulator. The test invokes the
function, then verifies the side effects (S3 object exists, DynamoDB item written,
SQS message receivable, etc.). All functions use `Timeout=30` to allow time for the
round-trip.

**Versioning test**: Publishing the same code twice returns the same version number
(Moto deduplicates). Fixed by updating the code between publishes. Also simplified
to not assert on version-specific code execution (in-process executor always uses
$LATEST code, not the pinned version's code).
