---
session: "8f3c1a"
timestamp: "2026-04-20T10:05:15Z"
model: "gpt-5-codex"
---

## Human

Implement native persistence for in-memory AWS services in this repo using
existing state/snapshot framework. Do not build new persistence subsystem, disk
cache layer, or event replay mechanism unless codebase proves existing
framework cannot support requirement.

First, read and follow repo guidance:
- `AGENTS.md`
- `README.md`
- `CLAUDE.md`
- `prompts/PROMPTLOG.md`
- `.github/workflows/ci.yml`

Goal:
- Make native SQS queues/messages and native EventBridge state survive
  existing snapshot/save/load flows and startup restore flows already
  supported by repo.
- Reuse same persistence pattern for native services that keep meaningful
  in-memory provider state.
- Keep solution generic to robotocore itself. Do not add app-specific or
  environment-specific logic.

Design constraints:
- Reuse `StateManager.register_native_handler(...)` and current
  `native_state.json` flow.
- Keep patch surface small and aligned with current architecture.
- Preserve existing native-service behavior as much as practical after
  restore.
- Prefer simple default-first control flow.
- No destructive git operations.

What to inspect before coding:
- `src/robotocore/state/manager.py`
- `src/robotocore/boot/components.py`
- `src/robotocore/services/sqs/provider.py`
- `src/robotocore/services/sqs/models.py`
- `src/robotocore/services/events/provider.py`
- `src/robotocore/services/events/models.py`
- `src/robotocore/services/events/rule_scheduler.py`
- Any existing state snapshot tests

Implementation requirements:
1. Register SQS as native persisted service through existing state manager
   hooks.
2. Ensure registration happens before startup load path so saved SQS state is
   actually consumed.
3. Serialize and restore enough native SQS state to make persistence useful
   for local dev and tests:
   - stores by account and region
   - queue definitions, attributes, tags, timestamps
   - visible, inflight, and delayed messages
   - receipt handle mapping
   - FIFO ordering state, including group queues and sequence continuity
   - behavioral trackers needed to avoid obvious regressions after restore
   - move-task state if needed for correctness
4. Rebuild in-memory queue objects correctly on load.
5. Keep implementation JSON-serializable through existing native state writer.
6. Avoid adding persistence for data that is truly unnecessary unless needed
   for correctness.
7. Apply same native persistence seam to EventBridge for state that is
   native and in-memory:
   - per-account/per-region stores
   - event buses, rules, targets, archives, replays, tags
   - provider-owned connections, API destinations, and endpoints
8. Keep debug/runtime-only state ephemeral where persistence would be
   incorrect:
   - EventBridge invocation log
   - EventBridge scheduler last-fired monotonic cache

Follow-up hardening requirements:
1. Address hot-path registration overhead so state-handler registration is not
   doing avoidable churn on steady-state request path.
2. Reduce provider-level private-attribute reach-through by moving
   snapshot/restore behavior into owner classes where practical.
3. Make FIFO restore defensively correct, including heap invariant handling
   when restoring group queues.
4. Tighten lock scope and simplify snapshot flow where safe.
5. Remove unnecessary serializer/deserializer indirection if it provides no
   real flexibility.
6. Simplify dataclass persistence where fields are primitive-safe.
7. Normalize native snapshot shape and versioning where branch scope grows:
   - top-level `schema_version`
   - explicit warnings on unexpected future versions
   - strict validation for restore invariants that should not be silently
     repaired

Testing requirements:
- Add focused unit tests for SQS native export/import round trip.
- Add focused test covering state-manager save/load round trip for SQS.
- Cover at least:
  - standard queue with visible/inflight/delayed messages
  - FIFO queue ordering and sequence continuation after restore
  - receipt-handle continuity after restore
  - expired inflight message becoming receivable after restore
  - multi-account and multi-region isolation
  - dedup cache survival after restore, including duplicate rejection
  - purge/delete behavioral tracker round trip
  - move-task and recently-deleted state round trip
  - DLQ behavior after restore
  - binary message attributes through disk-backed state-manager path
  - export/load idempotency
- Add focused EventBridge persistence tests covering at least:
  - buses, rules, targets, archives, replays, and tags round trip
  - multi-account and multi-region isolation
  - scheduled rules firing after restore with fresh scheduler cache
  - provider-owned connections/api-destinations/endpoints via disk-backed
    save/load
  - load replacement semantics
  - empty snapshot loads
  - ignored unknown top-level and dataclass fields
  - warnings on explicit future `schema_version`
  - failure on invalid default-bus identity during restore
- Tighten tests around real restore contracts and reduce brittle fixture/setup
  coupling.
- If compatibility/integration coverage is appropriate and cheap, add it. Do
  not add speculative tests for behavior not verified.

Verification requirements:
- Run targeted tests for changed area.
- Run relevant lint/format/type checks for changed files.
- If any repo-standard check cannot run, report exact command and blocker.
- Do not claim full validation if only targeted checks ran.

Documentation / PR hygiene:
- Update docs only if user-facing persistence behavior changes and docs are now
  inaccurate.
- Make prompt log and docs match resulting behavior.
- Add prompt log entry in `prompts/` per repo rules, sanitized and formatted
  correctly, so PR passes prompt-log requirement.

Output expectations:
- Make code changes directly.
- Then summarize:
  - what changed
  - what was verified
  - remaining risks or fidelity gaps

## Assistant

## Key decisions

**Use existing native state hooks**: Chose `StateManager.register_native_handler`
and `native_state.json` as persistence seam. This fit repo architecture, kept
patch surface small, and avoided inventing a second persistence system for
native providers.

**Register before load**: Wired native state-handler registration into boot path
so startup restore can actually consume native state before first request and
before background engines start. Without this, saved blobs would exist on disk
but never hydrate.

**Persist behaviorally meaningful SQS state**: Saved and restored queue/store
objects rather than request history. Coverage includes per-account/per-region
stores, queue definitions, visible/inflight/delayed messages, receipt maps,
FIFO group ordering state, dedup cache, sequence counters, move-task state, and
purge/delete cooldown trackers.

**Apply same seam to EventBridge**: EventBridge already had native in-memory
stores plus provider-owned globals. Added native persistence for buses, rules,
targets, archives, replays, tags, and connection/API-destination/endpoint
state, while intentionally skipping debug invocation logs and scheduler
runtime cache.

**Push persistence ownership into SQS classes**: Reduced provider-level
reach-through by adding `snapshot_state()` / `restore_state()` or
`from_snapshot()` methods on behavioral trackers, messages, move tasks, queues,
and stores. Provider now coordinates persistence instead of serializing private
fields by hand.

**Tighten concurrency and simplify structure**: Collapsed unnecessary
serializer/deserializer wrapper indirection, moved per-object snapshot logic
into owner classes, took FIFO snapshots under one mutex to avoid split-lock
inconsistency, and shortened `_store_lock` hold time during export by copying
store references first and serializing outside global lock.

**Simplify safe dataclass persistence only where primitives allow it**:
`MessageMoveTask` now uses dataclass helpers for snapshot/restore. `SqsMessage`
stayed explicit because it contains JSON-hostile fields like sets and may carry
binary message attributes. EventBridge dataclass restores filter unknown future
fields so older code can load newer snapshots without crashing.

**Update docs to match new semantics**: Existing docs said restart always loses
SQS state. Updated repo docs to say state is in-memory by default, with
optional snapshot/persistence support, and that native SQS and EventBridge now
participate in that flow.

**Strengthen tests around user-visible restore contracts**: Added focused unit
coverage for standard queues, FIFO queues, multi-account/multi-region
isolation, move tasks, recently-deleted state, purge/delete trackers,
receipt-handle continuity, expired inflight redelivery after restore, DLQ
routing after restore, disk-backed binary message attributes, and
export/load idempotency. Added EventBridge round-trip, replacement,
forward-compat, scheduler-after-restore, and provider-global persistence
tests.

**Improve fixture isolation**: Added direct test reset helpers for SQS provider
globals and StateManager singleton so test setup no longer depends on
`load_state({})` being healthy. This makes failures point at real restore bugs
instead of collapsing the whole test file.

**Reduce brittle test coupling**: Reworked heapify regression coverage to use a
small hand-built queue snapshot at queue boundary instead of mutating the full
exported nested snapshot payload. Split dense tests so failures isolate to one
behavior at a time.

**Validate snapshot shape explicitly on load**: Added `schema_version` to
native provider snapshots and load-time warnings for unexpected future
versions. EventBridge restore also validates that the restored default bus
identity matches the `(account_id, region)` store key instead of silently
rewriting mismatched snapshot data.

**Keep backward loads quiet, future loads loud**: Missing `schema_version`
means pre-versioned snapshot and loads silently. Explicit higher versions log a
warning and continue with v1 logic so local snapshots stay usable while making
unsupported forward loads visible. Applied symmetrically to EventBridge and
SQS.
