---
session: "dev-ux-conformance"
timestamp: "2026-10-10T04:55:00Z"
model: claude-opus-4-6
sequence: 5
---

## Human

Continue improving robotocore (same standing goal).

## Assistant

## Key decisions

**Batch 2 of the review findings** (verified in source, then fixed):

- Chaos-injected errors and the SQS error builder interpolated
  admin-authored / caller-authored text into XML bodies unescaped.
- The moto bridge classified ANY dispatch exception as 501 "not
  implemented" (a broken backend presented itself as a coverage gap) and
  forced moto's XML error bodies under an application/json content type.
- Health endpoints claimed "running" for SERVICES-disabled services.
- The `@connections` API returned None for unhandled methods (client
  crash); it answers 405 with Allow now.
- Under ENFORCE_IAM, an unreachable IAM backend surfaced as a 403 deny,
  blaming the caller for an emulator crash; it answers the 500 contract
  now.
- The compat warm-server guard now SKIPS those tests with a warning
  instead of aborting every whole-tree run — the split suites are
  process-global (moto singletons), documented in local-ci.sh; this was
  verified by sample runs (45 archives skipped under warm, 45 green
  under ALLOW=1, shard runs against a fresh server unaffected).

**Found en route**: the residual 841 failures in a bare whole-tree run
are cross-suite state pollution (each `tests/unit/services` file passes
in CI's isolated job) — that is the repository's known test-split
implementation, not a defect introduced by the batch; it is documented
as such in the script's header rather than silently retried.
