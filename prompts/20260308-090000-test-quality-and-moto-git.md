---
session: "test-quality-moto-git"
timestamp: "2026-03-08T09:00:00Z"
model: claude-opus-4-6
tools: [subagent, git-worktree]
sequence: 1
---

## Human

Select a few of our unit tests and review whether they're good tests.

## Assistant

### Key decisions

Reviewed 4 test files: `test_appsync_provider.py`, `test_batch_provider.py`, `test_audit_log_unit.py`, `test_chaos_middleware.py`.

**Audit log tests**: Excellent — no changes needed. Small, focused, tests one thing each, proper time bracketing.

**AppSync & Batch tests**: Good structure but weak assertions. Many tests assert `status_code == 200` without verifying the operation had any effect. A delete test that doesn't confirm the resource is gone could pass even if the operation is a no-op.

**Chaos middleware tests**: Good coverage but over-mocked. Every test had `with patch("robotocore.chaos.middleware.get_fault_store", ...)` nesting. Three tests verified the same error response body from different angles. One test asserted `inspect.iscoroutinefunction` — an implementation detail.

## Human

Write a plan to fix this. We should never over-mock.

## Human

We should also examine and assert on all parts of the return value/response, not just one part that we assume represents the whole.

## Assistant

### Key decisions

Used Explore agents to read the full response shapes from AppSync and Batch providers before writing the plan. Every assertion in the plan is grounded in actual provider return values, not guesses.

**Plan structure**: Three changes across three files. No new files, no abstractions.

1. AppSync & Batch: assert all response fields (not just one), verify side effects via follow-up calls
2. Chaos: extract autouse fixture, merge 3 duplicate tests into 1, delete implementation-detail test, strengthen all error tests to check full response shape
3. Audit log: no changes

## Human

Implement this plan on a git worktree.

## Assistant

### Key decisions

**Parallelization**: Launched 3 worktree agents simultaneously (one per file). Each agent got exact edit instructions with specific line numbers and assertion code.

**Merge conflict with AppSync**: The worktree agent worked from a stale base — it didn't have staged changes that split `test_tags_get` into `test_tags_get_nonexistent` (404) and `test_tags_get_existing_api` (200). The checkout from the worktree branch overwrote the staged changes. Fixed by restoring HEAD, then manually applying the worktree's assertion improvements while keeping the correct tags split.

**Chaos file lost in cleanup**: The worktree branch was cleaned up before I could checkout the chaos file changes. Rewrote it manually — cleaner than trying to recover. The autouse fixture pattern is straightforward.

**Batch agent went beyond scope**: Added new test classes (tag lifecycle, account isolation, atomicity) and fixed a real bug (`_stores.get(REGION)` → `_stores.get(f"{ACCOUNT}:{REGION}")`). Kept the changes since they're high quality and test real behaviors.

**Final counts**: 93 tests pass (33 AppSync + 40 Batch + 20 Chaos). Chaos went from 22 → 20 tests (3 merged into 1, 1 deleted).

## Human

We've got Moto vendored in the filesystem but that breaks remote docker build. Update our skills such that we always push to my remote Moto fork (github.com/JackDanger/moto) on the correct robotocore/all-fixes branch. Then reference that in Docker build and GHA and everywhere.

## Assistant

### Key decisions

**Root cause**: `pyproject.toml` had `moto = { path = "vendor/moto", editable = true }`. The Dockerfile doesn't copy `vendor/moto/` into the build context, so `uv sync` fails with "Distribution not found."

**Chose git URL over copying submodule**: Could have added `COPY vendor/moto vendor/moto` to Dockerfile, but that would make the Docker context huge and couple the build to the submodule. Instead, changed the uv source to `{ git = "https://github.com/JackDanger/moto.git", branch = "robotocore/all-fixes" }`. Docker/CI install from the remote; local dev gets it from the same remote (fast with uv's cache).

**Kept vendor/moto as convenience checkout**: Scripts like `generate_parity_report.py`, `batch_register_services.py`, and `service_health_matrix.py` read the Moto filesystem to count operations and enumerate backends. These are dev-only tools. The submodule stays for agent/dev convenience but is not used at install time.

**Kept `submodules: recursive` in CI**: Some CI jobs run scripts that read `vendor/moto/` and `vendor/localstack/`. Removing submodule checkout would break those. It's cheap to keep.

**Pushed stale moto commits**: The remote `robotocore/all-fixes` was 2 commits behind local. Pushed with `git push jackdanger HEAD:robotocore/all-fixes` before making the pyproject.toml change.

**overnight.sh update**: After moto fixes, the script now pushes to the fork and runs `uv lock` before committing robotocore. Stages `uv.lock` instead of `vendor/moto`.

**Dockerfile**: Added `git` to apt-get install — uv needs it to clone from GitHub.

**Verified end-to-end**: `uv sync` works, 3441 unit tests pass, Docker builds, container boots with all 147 services running.
