---
session: "20f01d16"
timestamp: "2026-03-15T22:16:00Z"
model: claude-sonnet-4-6
---

## Human

The PR #161 still has many failing tests. Check all runs and fix all problems.

## Assistant

## Root cause analysis

After the StepFunctions and FIFO fixes from the previous session, PR #161 still had
8 failing jobs: all Lambda-invocation tests across event_chain, event_driven,
ecommerce_order, content_mgmt, and data_pipeline.

The common pattern: every failing test followed `Lambda did not process X within Ns`
and returned `None` or `0` from a DynamoDB get/scan. Tests that did NOT involve
Lambda (EB→SQS, alarm→SNS, sqs-delete-after-success) all passed.

**Key diagnostic**: `test_sqs_message_deleted_after_success` PASSES. Its Lambda
returns `{"statusCode": 200}` without any boto3 calls. But `test_sqs_message_triggers_lambda`
FAILS — its Lambda calls `boto3.client("dynamodb", endpoint_url="http://localhost:4566")`.

**Root cause**: `execute_python_handler` in `executor.py` builds `invocation_env` from
`os.environ._real` (the server's real environment), then sets only Lambda-specific vars
(`AWS_LAMBDA_FUNCTION_NAME`, `AWS_REGION`, etc.). It does NOT set `AWS_ACCESS_KEY_ID`.

In CI, the server is started without ambient AWS credentials:
```bash
ROBOTOCORE_PORT=4566 HTTPS_DISABLED=1 DNS_DISABLED=1 uv run python -m robotocore.main &
```

When a Lambda function (running in-process) calls `boto3.client("dynamodb", endpoint_url=...)`,
boto3 traverses the standard credential chain and finds nothing → raises `NoCredentialsError`
before making any HTTP request. The error is caught by `_invoke_lambda_with_result`'s
exception handler → `success=False` → ESM does not retry → test times out.

Locally this works because the user's shell has real AWS credentials or `~/.aws/credentials`.

## Fix

Added three `setdefault` calls in `execute_python_handler` to inject dummy credentials
that boto3 will find, without overriding user-supplied values:

```python
invocation_env.setdefault("AWS_ACCESS_KEY_ID", "testing")
invocation_env.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
invocation_env.setdefault("AWS_SESSION_TOKEN", "testing")
```

The emulator ignores credential values (no auth validation), so "testing" is sufficient.

## Deployment

1. Committed fix to `jackdanger/uv-sync`
2. Cherry-picked onto `fix/app-integration-tests-timeout` (PR #161's branch)
3. Force-pushed → new CI run triggered (run ID 23122712764)
