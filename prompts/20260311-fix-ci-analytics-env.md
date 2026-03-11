---
role: assistant
timestamp: "2026-03-11T21:20:00Z"
session: fix-ci-analytics-env
sequence: 1
---

# Fix CI analytics env var isolation in tests

## Problem
`TestDetectCIProvider` tests for GitLab, Jenkins, CircleCI, and generic CI all fail when run inside
GitHub Actions because `GITHUB_ACTIONS=true` is always set in the environment. `mock.patch.dict` with
`clear=False` adds new vars but doesn't remove existing ones, so `detect_ci_provider()` always matches
GitHub Actions first.

## Fix
- Use `monkeypatch.delenv` to remove `GITHUB_ACTIONS` and `GITHUB_RUN_ID` before non-GitHub tests
- Use `monkeypatch.delenv("GITHUB_RUN_ID")` for the without-run-id edge case test
