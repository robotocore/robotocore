---
session: "pr-270-nodejs-multi-version"
timestamp: "2026-05-11T22:30:00Z"
model: claude-sonnet-4-6
sequence: 5
---

## Human

Consider how we can support the various versions of node in PR 270. Also, read all PR 270 comments and implement fixes. Make it awesome, no matter how much work you have to do.

## Assistant

## Key decisions

### True multi-version Node.js support

**Root cause of reviewer concern**: The original PR installed Debian's `apt-get nodejs` package, which is Node 18.x on `python:3.12-slim` (bookworm). The PR advertised `nodejs18.x`/`nodejs20.x`/`nodejs22.x` support but all three ran on the same binary — diverging from AWS where each identifier runs on its named version.

**Approach chosen**: Install three versioned Node.js binaries from official Docker images using multi-stage COPY, plus a default `node` alias pointing to 20 (current LTS):

```dockerfile
COPY --from=node:18-slim /usr/local/bin/node /usr/local/bin/node18
COPY --from=node:20-slim /usr/local/bin/node /usr/local/bin/node20
COPY --from=node:22-slim /usr/local/bin/node /usr/local/bin/node22
COPY --from=node:20-slim /usr/local/bin/node /usr/local/bin/node
```

Each binary is ~65MB and self-contained (links against standard Debian libs already in `python:3.12-slim`). Total overhead: ~200MB vs ~65MB for a single version.

**Version-aware executor**: `NodejsExecutor` now takes `runtime: str = ""` at construction and has `_resolve_binary()` that maps `nodejs18.x` → `node18`, `nodejs20.x` → `node20`, `nodejs22.x` → `node22`, falling back to plain `node` if the versioned binary isn't present (covers dev environments with just `node` on PATH).

**Per-version executor singletons**: `get_executor_for_runtime` now keys Node.js executors by the full runtime string (`"nodejs18.x"`, `"nodejs20.x"`, `"nodejs22.x"`) instead of by family. Each version gets its own `NodejsExecutor(runtime=...)` instance cached independently.

### CI smoke test bugs fixed

1. **Double-read of streaming body**: The original assert did `json.loads(resp["Payload"].read())` in the f-string, then the next line tried `resp["Payload"].read()` again — the second read always returns empty bytes. Fixed by reading the payload once before any assertions.

2. **Missing try/finally cleanup**: IAM roles and Lambda functions could leak if assertions failed mid-test. Wrapped each runtime test in `try/finally` with best-effort cleanup.

3. **Test all three versions**: The CI smoke test now loops over `nodejs18.x`, `nodejs20.x`, `nodejs22.x`, invokes each, and also verifies that `process.version` major matches the runtime identifier (e.g., `nodejs20.x` must report `v20.*`).

### What was NOT done

ES Module support (`.mjs`, `"type": "module"`) — the bootstrap still uses CommonJS `require()`. This is a separate follow-up item noted in the original PR description.
