---
role: assistant
timestamp: "2026-03-11T16:00:00Z"
session: state-consistency
sequence: 1
---

# State Consistency Guarantees

## What
Added concurrent snapshot save safety with file locking and atomic writes
to prevent state corruption when multiple saves happen simultaneously.

## Design Decisions
- Used `fcntl.flock` for cross-process file locking (POSIX-only, acceptable for dev use)
- Atomic writes via temp file + rename to prevent partial state files
- Lock timeout of 10 seconds to avoid deadlocks
- Tests verify concurrent save/load doesn't corrupt state
