---
session: "snapshot-strategies"
timestamp: "2026-03-11T05:38:03Z"
model: claude-opus-4-6
---

## Human

Implement configurable snapshot save/load strategies for robotocore's state
management. Support SNAPSHOT_SAVE_STRATEGY (on_shutdown, on_request, scheduled,
manual) and SNAPSHOT_LOAD_STRATEGY (on_startup, on_request, manual) env vars.
Include change tracker, scheduled background saver, debouncing, and lazy load.
Write tests first.

## Assistant

## Key decisions

**Change tracker as separate module**: Created `src/robotocore/state/change_tracker.py`
with a thread-safe dirty flag. Tracks mutations by HTTP method -- GET/HEAD/OPTIONS
are read-only, everything else marks dirty. Management paths (/_robotocore/) are
excluded since they don't affect AWS state.

**Strategy as enums, not strings**: Used Python enums (SnapshotSaveStrategy,
SnapshotLoadStrategy) for type safety. InvalidStrategyError provides helpful
messages listing valid values.

**Backward compatibility**: Legacy env vars (ROBOTOCORE_PERSIST, PERSISTENCE)
still work alongside the new strategy system. The new strategy hooks are additive.

**Scheduled saver uses threading.Event.wait()**: The background thread uses
`Event.wait(timeout=interval)` instead of `time.sleep()` so it can be stopped
cleanly without waiting for the full interval.

**Debouncing preserved**: The on_request save strategy reuses the existing
`save_debounced()` method (max 1 save/second) to avoid thrashing disk.

**38 new tests**: 27 unit tests covering all strategies, change tracker behavior,
env var parsing, and error cases. 4 semantic integration tests verifying end-to-end
flows (scheduled tick, on_request save/load, manual mode).
