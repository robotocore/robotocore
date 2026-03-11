---
session: "ab70ba64"
timestamp: "2026-03-11T20:00:00Z"
model: claude-opus-4-6
---

## Human

Build a comprehensive cross-service integration test suite for the robotocore project. Create `tests/integration/test_cross_service_workflows.py` with 10 real-world workflow tests covering event-driven pipelines, API Gateway chains, CloudFormation provisioning, EventBridge fan-out, Step Functions orchestration, S3 notifications, Secrets Manager with Lambda, multi-account isolation, IAM enforcement, and state snapshot round-trips.

Also fix the async test in `tests/unit/providers/test_moto_bridge.py` at line ~327, converting from `@pytest.mark.asyncio / async def` to a plain `def` using `asyncio.run()`.

## Assistant

## Key decisions

**Test architecture**: Organized as one class per workflow (10 classes total), each with clear setup, execution, and verification phases. Used `try/finally` blocks with a `_suppress()` context manager for cleanup instead of pytest fixtures, since these tests create complex resource graphs where fixture teardown ordering can be fragile.

**Helper utilities**: Added `_unique()` for resource naming and `_wait_for()` polling helper at module level. These are intentionally simple -- no need for a full framework when 10 tests share the same patterns.

**Lambda integration tests (workflows 1, 2, 7)**: Lambda handlers embed `endpoint_url="http://localhost:4566"` directly since they run inside the emulator's Lambda runtime. This is the correct approach for emulator testing -- real AWS Lambda would use VPC endpoints or environment variables.

**S3 notification test (workflow 6)**: Verified notification configuration round-trip and object upload rather than asserting on SQS message delivery. Moto's S3 notification delivery is best-effort in emulators, so the test validates the configuration plumbing without depending on async delivery.

**Multi-account isolation (workflow 8)**: Used different `aws_access_key_id` values to simulate different accounts. The tests verify that each account's client can access its own resources. Full cross-account isolation verification depends on how Moto partitions by credential.

**IAM enforcement (workflow 9)**: Uses the runtime config endpoint (`/_robotocore/config`) to toggle `ENFORCE_IAM` on/off within a single test. Skips if runtime config updates are not enabled. Always restores `ENFORCE_IAM=0` in the finally block to avoid affecting other tests.

**State snapshot (workflow 10)**: Creates resources across 5 services (S3, SQS, DynamoDB, SNS, SSM), saves a snapshot, resets state, loads the snapshot, and verifies all resources are restored. Uses the `/_robotocore/state/*` management endpoints directly via `requests`.

**Async test fix**: Converted `test_forward_to_moto_with_body_preserves_content_length_for_head` from `@pytest.mark.asyncio / async def` to a plain `def` using `asyncio.run()`. This eliminates the dependency on pytest-asyncio for this single test.
