---
role: assistant
timestamp: "2026-03-11T08:00:00Z"
session: code-quality-fixes
sequence: 1
---

# Code Quality Fixes

## Changes
Fixed assert-with-side-effects in `tests/unit/test_fault_rules.py` — extracted side-effecting calls from assert statements to separate lines.

## Decisions
- Preferred extracting to local variable over restructuring the test, keeping the fix minimal
