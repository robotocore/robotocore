# S3 Semantic Audit Baseline

## Request

Implement the epic PR slice of the S3 launch-to-semantics coverage audit:

- create a dedicated git worktree and branch
- add a canonical S3 launch catalog
- add an S3 semantic audit script and generated outputs
- add a connectivity matrix
- add tests and documentation
- go through the PR flow

## Notes

- The audit is intentionally non-invasive: no runtime S3 behavior changes in this PR.
- The worktree did not contain `vendor/moto/`, so the audit falls back to compat-derived S3 operations when the parity script collapses to native-only counts.
- Baseline audit result in this PR: 9 launch families passing local evidence checks, 3 failing (`CreateSession`, `RenameObject`, `WriteGetObjectResponse`).
