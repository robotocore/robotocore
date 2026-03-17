---
session: "1f7892b1"
timestamp: "2026-03-17T02:08:57Z"
model: claude-opus-4-6
---

## Human

Implement the gap surfacing system plan: build automated detection that catches
response shape mismatches, error format bugs, and state inconsistencies before
users report them. Phases: (1) response shape validator using botocore output
shapes, (2) error contract checker per protocol, (3) CRUD lifecycle test
generator and AWS snapshot recorder.

## Assistant

## Key decisions

**Shared param_filler module**: Extracted `auto_fill_params()`, `fill_structure()`,
`KNOWN_PARAMS` from `probe_service.py` into `scripts/lib/param_filler.py` so both
the existing probe script and the new validators can share it without duplication.

**Recursive shape walker**: Built `src/robotocore/testing/shape_validator.py` that
walks botocore output shapes recursively — structures, lists, maps, scalars. Reports
violations at three severity levels: error (missing required, type mismatch), warning
(missing optional), info (extra keys). Validates first list element as spot check.

**Error contract approach**: Rather than testing all operations, the error contract
validator triggers known "not found" errors (18 services with pre-defined trigger ops)
and validates both the boto3-level error structure and the raw HTTP wire format per
protocol (JSON `__type`, XML `<ErrorResponse>`/`<Error>`/`<Response>`, Content-Type).

**CI strategy**: Top 20 services in regular CI (fast, blocking), all 147 in nightly
(comprehensive, advisory with `|| true`). This catches regressions without slowing
every push.

**Lifecycle test generator**: Outputs commented scaffold code rather than runnable
tests, since CRUD params are service-specific and need manual tuning. This gives
humans a starting point without generating broken tests.
