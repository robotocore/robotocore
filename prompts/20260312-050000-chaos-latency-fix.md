---
session: "find-every-problem"
timestamp: "2026-03-12T05:00:00Z"
model: claude-opus-4-6
sequence: 2
---

## Human

Find every problem with robotocore and fix it.

## Assistant

## Key decisions

**Bug**: Chaos latency injection used `loop.create_task(asyncio.sleep(...))` which
scheduled the sleep as a fire-and-forget background task. The HTTP response was sent
before the delay completed, so latency injection had zero effect.

**Fix**: Replaced with `time.sleep()`. The handler chain runs synchronously inside
`asyncio.to_thread()`, so a synchronous sleep blocks only the current request thread
without blocking the event loop. This is the simplest correct fix.

**Tests updated**: Mocked `time.sleep` instead of `asyncio.run` in unit tests.
