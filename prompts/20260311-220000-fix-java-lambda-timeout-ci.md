---
session: "fix-java-timeout-ci"
timestamp: "2026-03-11T22:00:00Z"
model: claude-opus-4-6
---

## Human

PR #121 (java-lambda-compat-tests branch) has a CI failure in the compatibility-tests job.
The `TestJavaTimeout::test_timeout_exceeded` test fails with:
`assert ('timed out' in 'none' or 'timeout' in 'none')`.
The error_msg is "None" (string) instead of a timeout message. Fix it.

## Assistant

Root cause: `run_subprocess()` in `base.py` returned `(None, "Task.TimedOut", logs)` on
`subprocess.TimeoutExpired`. The `None` result was serialized as JSON `null`, which the
test client parsed back as Python `None`, then `str(None)` = `"None"`.

The docker executor and in-process executor both correctly return a structured error dict
`{"errorMessage": "...", "errorType": "Task.TimedOut"}` on timeout. Only the subprocess
path in `runtimes/base.py` was returning `None`.

Fix: return a structured error dict from `run_subprocess` on `TimeoutExpired`, matching
the pattern used by `docker_executor.py` line 326 and `executor.py` line 668-672.
