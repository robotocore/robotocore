---
session: "ec77da82"
timestamp: "2026-05-11T00:04:00Z"
model: claude-sonnet-4-6
---

## Human

For PR 270 - write a failing test case that would have caught it. Clearly we didn't actually run a node lambda. Make sure we do, for all languages

## Assistant

The gap was two-fold:

1. `tests/compatibility/test_lambda_nodejs_compat.py` had `pytestmark = pytest.mark.skipif(shutil.which("node") is None, ...)`. This checks the LOCAL machine's PATH — irrelevant for a compat test that talks to a server over HTTP. If node is missing from the Docker image, the test would only catch it if developers happened to run compat tests against the Docker container (not the local dev server). In CI the compat tests ran against the local server, which inherits the CI machine's PATH (node available), so all compat tests passed even with node missing from Docker.

2. The Docker build CI smoke test in `.github/workflows/ci.yml` only exercised S3 and SQS against the built Docker image. Lambda was never invoked against Docker in CI.

Fixes:
- Removed the `shutil.which("node")` skip from the compat test. Compat tests should always try to invoke Node.js Lambda via the server; if the server can't do it, that's a test FAILURE, not a skip.
- Added Python Lambda and Node.js Lambda invocation tests to the `docker-build` CI smoke test. These spin up the actual built Docker image and assert that both runtimes work end-to-end.

The Node.js CI smoke test would have FAILED before PR #270 because the Docker image lacked the `node` binary, producing a `FunctionError` response.
