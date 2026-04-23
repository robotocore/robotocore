---
session: 20260403-171614
slug: codecommit-edge-cases
service: codecommit
type: compat-tests + moto-fixes
---

## Prompt

Strengthen CodeCommit compat tests with edge cases and fix Moto bugs found during testing.

## Changes

Added 45 new tests (79 → 124 total) to `tests/compatibility/test_codecommit_compat.py`:

- Pagination tests for list_repositories and list_pull_requests
- Edge cases: unicode values, max-length descriptions, invalid names, idempotent deletes
- ARN/URL format assertions
- Batch operations with missing items
- Behavioral fidelity: FF merge updates branch HEAD, evaluate approval rules with actual rule satisfaction logic, commit parent chains, status filters for PRs, update-name preserves repository ID, tag unicode values

## Moto fixes (pushed to JackDanger/moto)

Three bugs found and fixed:
1. `batch_get_commits`: errors field now returns `BatchGetCommitsError` dicts (`{commitId, errorCode, errorMessage}`) instead of raw strings
2. `merge_branches_by_fast_forward`: actually updates destination branch HEAD to source commit ID (was generating random IDs without updating state)
3. `evaluate_pull_request_approval_rules`: rules now properly classified into satisfiedRules/notSatisfiedRules based on actual approval count vs needed
