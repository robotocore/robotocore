---
session: "app-tests-timeout"
timestamp: "2026-03-15T00:00:00Z"
model: claude-opus-4-5
---

## Human

Fix that this seems to time out. Split it up?
https://github.com/robotocore/robotocore/actions/runs/23101193794/job/67113269963

## Human

make a git worktree, commit the ./prompts/ promptlog, and open a PR

## Assistant

## Key decisions

**Root cause**: The app integration tests job runs all ~80 test files in a single job with a 15-minute timeout. Total runtime exceeded 15 minutes, causing the job to be cancelled.

**Fix**: Split into a matrix strategy with 14 parallel jobs, one per app directory (`cicd_pipeline`, `content_mgmt`, `data_pipeline`, etc.). Each job runs `pytest tests/apps/${{ matrix.app }}/` instead of the full suite, so wall time is dominated by the slowest app (~5–10 min) rather than the sum of all tests.

**Worktree/PR pattern**: Created worktree at `../robotocore-app-tests-fix` on branch `fix/app-integration-tests-timeout`, committed workflow change + this prompt log, pushed, and opened PR.
