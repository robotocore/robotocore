---
session: "migration-parity"
timestamp: "2026-03-12T00:34:33Z"
model: claude-opus-4-6
tools: [subagent, git-worktree]
sequence: 3
---

## Human

[Continuation of migration parity session — see sequence 1 for full chain]

Implement all of those features — each with a subagent on a worktree.

## Assistant

## Key decisions

**This prompt log covers versioned snapshots** (PR #110) — the Cloud Pods equivalent.

**Auto-incrementing versions**: Each `save` to the same name bumps the version
counter. This matches the Cloud Pods mental model where you `save` repeatedly and
can `load` any prior version. The alternative (explicit version numbers) adds
friction for the common case.

**Backward-compatible API**: The existing `POST /_robotocore/state/save` endpoint
now returns `{"name": "x", "version": 3}` instead of just `{"status": "saved"}`.
Existing clients that ignore extra fields won't break. `load` without a version
parameter loads latest, preserving existing behavior.

**In-memory storage**: Versions are stored in a nested dict structure. No disk
persistence yet — that's a separate concern. The snapshot data includes metadata
(timestamp, services list, size estimate) to support future UI and CLI tooling.

**Thread safety**: All snapshot operations are guarded by `_snapshot_lock`. The
test suite includes a concurrent save test (4 threads × 10 saves) to verify.
