---
session: "dev-ux-conformance"
timestamp: "2026-10-10T04:05:00Z"
model: claude-opus-4-6
sequence: 4
---

## Human

Continue improving robotocore (same standing goal). You ran adversarial
reviews; work the findings.

## Assistant

## Key decisions

**Review round 1 verified before fixing.** Two read-only review agents
returned 24 findings. Each high-impact claim was re-verified in source
(and by running a request) before this batch. That re-verification
caught a misleading claim and, underneath it, a deeper true defect: the
x-robotocore-diag header carried an unescaped exception message, so an
em dash inside any exception's text crashed Starlette's latin-1 header
encoding and turned the original 4xx into an encoding 500. Header
values are now ASCII-fenced; any message with non-latin-1 characters is
safe to report.

**Batch fixed (one round of the loop, one PR):**

1. Provider crashes route through the error normalizer (was: plain-text
   'Internal Server Error').
2. 500 responses use AWS's InternalError/InternalFailure codes instead
   of the Python class name; the exception remains in the diag header.
3. Unregistered services (typed into the credential scope) answer 501
   NotImplemented — the AGENTS.md 'only 501 is a gap' contract — while the
   SERVICES operator filter answers with a filter-naming message.
4. The unsigned form-encoded fallback only routes real STS operations to
   STS (it previously claimed any unsigned form POST, so a CreateQueue
   posted unsigned reported the wrong service).
5. The unrelated 400-in-the-middle 'see' pointer to a repository file that
   an HTTP client cannot open was dropped (kept hints local to the body).

The gateway unit tests that asserted the poor contract were updated with
the corrected expectations, plus five new tests covering the fresh
behaviors (moto-path crash → 500 InternalFailure; unregistered service →
501 NotImplemented; SERVICES filter message names the filter; unsigned
form POST routes only for STS ops with the Action in body or query).
