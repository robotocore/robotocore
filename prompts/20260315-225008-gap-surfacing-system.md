---
name: Gap Surfacing System
description: Five-layer system to make hidden implementation bugs impossible to miss
type: feature
role: assistant
timestamp: 2026-03-15T22:50:08Z
session: gap-surfacing
sequence: 1
---

## Human Prompt

Implement the Gap Surfacing System plan: wire name gate, weak assertion detector, fallthrough
coverage auditor, async race lint rule, and or-default masking detector.

## What Was Built

Five layered detection mechanisms, each wired into pre-commit and/or CI:

### 1. Wire Name Gate
- Added `fix_moto_param_names.py --all` to `.git/hooks/pre-commit` (runs when `src/` is staged)
- Added to CI `test-quality` job
- Catches Moto `_get_param()` casing bugs (e.g. `"logGroupName"` vs `"LogGroupName"`)

### 2. Weak Assertion Detector (`validate_test_quality.py`)
- New `_is_key_presence_assert()` helper: detects `assert "X" in response` patterns
- New classification: `weak_assertion` — all assertions are key-presence only, no value checks
- New `--max-weak-assertion-pct` flag (CI threshold: 25%; current baseline: 15.7%)
- Added `weak_assertion` column to JSON output and text report

### 3. Fallthrough Coverage Auditor (`scripts/audit_fallthrough.py`)
- New script: for each native provider with `_ACTION_MAP`, finds all botocore operations
  NOT in the map (fallthrough to Moto), then checks compat test coverage
- `--all`, `--service`, `--json`, `--max-uncovered`, `--uncovered-only` flags
- CI gate: `--max-uncovered 400` (current: 392 uncovered; ratchet down over time)
- Baseline: 10 providers, 1174 fallthrough ops, 782 covered (66.6%)

### 4. Async Race Lint Rule (`lint_project.py` check 11)
- New `async-race-test` check: finds tests that trigger async op then immediately read state
- Trigger verbs: `start_execution`, `invoke`, `create_pipe`, `run_task`, `send_message`, etc.
- Read verbs: `describe_execution`, `get_function`, `describe_pipe`, `get_query_results`, etc.
- WARNING severity; detected 13 real races on first run across unit + compat tests

### 5. `or`-Default Masking Detector (`fix_moto_param_names.py --check-defaults`)
- New `find_or_defaults()` function and `--check-defaults` flag
- Classifies: `silent_mismatch` (wrong wire name + `or` default silences error) vs
  `masked_default` (correct name but `or` default hides None return)
- Phase 2 — informational only, not yet in CI gate

## Key Decisions

- Initial CI thresholds set permissively (weak_assertion: 25%, fallthrough: 400) to avoid
  blocking CI on day one. These are ratchet thresholds: only ever decrease.
- Wire name gate exits non-zero only on CASING bugs (auto-fixable), not UNKNOWN params
  (which require manual review and may be intentional Moto internals).
- Async race check is WARNING not ERROR — some "immediate reads" may be correct
  (synchronous operations, mocked time in unit tests).
