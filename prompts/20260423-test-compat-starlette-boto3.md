---
session: 20260423
slug: test-compat-starlette-boto3
type: ci-fix
---

## Context

Two dependabot PRs (#248 starlette >=1.0.0, #246 boto3 >=1.42.88) had failing CI.

## Changes

### Starlette 1.0 fix (`tests/unit/config/test_runtime_integration.py`)

Starlette 1.0 removed `Router.on_startup` and `Router.on_shutdown` lists. Tests used `.clear()` on these to prevent SMTP binding during tests. Replaced with `AsyncMock` patches on `_start_background_engines` and `_shutdown` — works with all starlette versions and is clearer about intent.

### Boto3 >=1.42 fix (`tests/compatibility/test_bedrock_compat.py`)

Boto3 >=1.42 dropped `inputTags` from the `guardrailInferenceConfig` shape for `PutEnforcedGuardrailConfiguration`. Removed the parameter from the test.
