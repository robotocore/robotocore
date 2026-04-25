---
title: "Fix: beam-search bugs across gateway, providers, and services"
date: 2026-04-23
phase: fix
tags: [gateway, lambda, ssm, ecr, kinesis, ses, scheduler, batch, stepfunctions, config]
---

## Human

Beam search through the codebase with ~20 haiku agents, find bugs, make a PR.

## What was done

Fixed 8 distinct bugs found by parallel haiku agents scanning different subsystems:

**gateway/app.py** — `int(query_params.get("limit", "100"))` at three endpoints (audit, emails, iam-policy) raises unhandled `ValueError` on non-numeric input. Wrapped with try-except → 400.

**ssm/provider.py, ecr/provider.py, kinesis/provider.py, ses/sesv2_provider.py** — `json.loads(body)` calls without error handling, same pattern fixed throughout the project. Return 400 with error detail.

**scheduler/provider.py:514** — `invoke_lambda_async(arn, payload.encode(), ...)` passes bytes but the function signature expects `dict`. Changed to `json.loads(payload)`.

**batch/provider.py:505** — `_cancel_job` set `job["status"] = "FAILED"` instead of `"CANCELLED"`. AWS Batch returns CANCELLED for cancelled jobs.

**stepfunctions/asl.py:399,414** — `.waitForTaskToken` deadline was hard-capped at 30 seconds (`min(timeout, 30)`). This broke state machines with `TimeoutSeconds > 30`. Cap removed.

**config/runtime.py:60-61** — `_history` was initialized twice: first as `{}` (wrong type, suppressed with `# type: ignore`), then immediately reassigned to `[]`. Removed the dead first line.

**tests/unit/services/test_lambda_executor_review.py:165** — `@pytest.mark.xfail` removed. The underlying race condition was already fixed; the test passes consistently.
