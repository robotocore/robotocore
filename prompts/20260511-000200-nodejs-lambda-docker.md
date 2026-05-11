---
session: "20260511-open-issues"
timestamp: "2026-05-11T00:02:00Z"
model: claude-sonnet-4-6
sequence: 3
---

## Human

One by one, solve each open GitHub issue with a PR. (Continuation — working on issue #212.)

## Assistant

## Key decisions

**Issue #212: Node.js Lambda support**

Investigation revealed that Node.js Lambda support is already fully implemented in code:
- `src/robotocore/services/lambda_/runtimes/node.py` — `NodejsExecutor` class
- `src/robotocore/services/lambda_/runtimes/__init__.py` — maps `nodejs*` runtime strings to `NodejsExecutor`
- `src/robotocore/services/lambda_/runtimes/bootstraps/bootstrap.js` — JavaScript bootstrap that reads event from stdin, invokes the handler, writes result to stdout
- `tests/compatibility/test_lambda_nodejs_compat.py` — comprehensive compat tests (already skipped if `node` not found on PATH)

The only thing missing: **Node.js is not installed in the Docker image**. In the Docker container, `shutil.which("node")` returns `None`, so `NodejsExecutor.execute()` immediately returns `(None, "Runtime.InvalidRuntime", "Node.js not installed")`.

**Fix**: Add `nodejs` to `apt-get install` in the Dockerfile runtime stage.

**Why `nodejs` from Debian repos** over alternatives:
- NodeSource setup script: requires fetching external script during build (security concern, network dependency)
- `COPY --from=node:20-slim /usr/local/bin/node`: pulls from Node.js Docker image, adds ~85MB for just the binary, and we need to ensure all dynamic lib dependencies are available
- `apt-get install -y --no-install-recommends nodejs`: simplest, uses Debian bookworm's Node.js 18.x package, well-tested, minimal footprint (~65MB)

Debian bookworm (the base of `python:3.12-slim`) ships Node.js 18.x in the `nodejs` package. This is the LTS version that AWS Lambda `nodejs18.x` targets, and it runs `nodejs20.x` and `nodejs22.x` handler code correctly for the vast majority of use cases (language APIs are stable across these versions).

**README update**: Added Node.js example to the "Common patterns" section alongside the Python Lambda example, and updated the Lambda row in the feature table to explicitly list supported runtimes.

**What the bootstrap.js does NOT yet support**: ESM (`.mjs` files or `"type": "module"` in package.json). The bootstrap uses CommonJS `require()` only. This would require detecting ESM and using dynamic `import()` instead. Deferred — the vast majority of Lambda functions use CommonJS. ESM support is a clear follow-up.
