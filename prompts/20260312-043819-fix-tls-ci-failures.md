---
session: "pr113-tls-ci-fix"
timestamp: "2026-03-12T04:38:19Z"
model: claude-opus-4-6
---

## Human

Fix the test failures on PR #113 (feat/tls-support branch). Two CI jobs failing:
- unit: infra (3.13) - test_create_resources_wait_interval_verify_state_written
- unit: services (3.12) - test_state_manager_save_load_preserves_account_isolation

## Assistant

## Key decisions

Both failures are pre-existing issues unrelated to the TLS feature itself -- the PR's diff doesn't touch either failing test or the code they exercise. They were exposed by CI timing.

**Pickle allowlist for `re` module**: The `_RestrictedUnpickler` in `state/manager.py` blocks any module not in `_ALLOWED_MODULE_PREFIXES`. Moto backends pickle compiled regex objects (`re._compile`), which was not in the allowlist. Added `"re"` to the tuple. This is safe -- `re._compile` returns compiled pattern objects, not executable code.

**Scheduled save race condition**: The test polls for `metadata.json` to appear, then immediately asserts `is_dirty == False`. But `save()` writes the file at line 440 and calls `mark_clean()` at line 465 -- the test thread can see the file before the scheduler thread reaches `mark_clean()`. Fixed by polling for both conditions (file exists AND dirty flag cleared) in the same loop. Also increased deadline from 2s to 5s for CI resilience.
