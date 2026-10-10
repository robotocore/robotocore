---
session: "dev-ux-conformance"
timestamp: "2026-10-10T07:30:00Z"
model: claude-opus-4-6
sequence: 8
---

## Human

Continue improving robotocore (same standing goal).

## Assistant

## Key decisions

**PERSISTENCE=1 auto-save off the event loop.** The second audit's
performance finding: the debounced auto-save ran inline in the request
coroutine and the dump serializes every backend and hits fsync-class disk I/O;
a request could wedge every other in-flight request behind one full snapshot —
worse if the scheduled saver thread already holds the save lock. The fix is
the same contract the `/state/save` endpoints already use: dispatch through
`asyncio.to_thread`.

**How the test discriminates** (two self-corrections worth noting): a first
draft asserted the health probe's own latency in a racing window, but the
pre-sleep ramp absorbed the blocked interval — the assertion passed with the
defect still present. The final measurement runs the health probe against the
over-all clock: with the inline save the loop is wedged until the dump
finishes (~1.5s), so a health request cannot finish inside the save's own
duration; the patched code answers health while the save is mid-flight, and
the stub manager asserts the save actually ran. Verified discriminator: test
fails on the reverted input path, passes with the fix.

**Deferred consciously**: SQS `WaitTimeSeconds` long-polls sleeping inside
the shared default executor (the second performance finding) is a sizing/pool
design change that needs a concurrency benchmark to size correctly — the
bounded executor's right size depends on the workload mixture, so it waits
for its own measured slice rather than a guess.
