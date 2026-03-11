---
session: "acd822cb"
timestamp: "2026-03-11T06:00:00Z"
model: claude-opus-4-6
---

## Human

Implement IAM Policy Stream for robotocore -- a real-time log of IAM policy
evaluations showing which policies were checked and whether access was
granted/denied. Includes ring buffer storage, management API endpoints,
filtering by principal/action/decision, summary aggregation, and
least-privilege policy generation.

## Assistant

## Key decisions

**Modeled after AuditLog**: Used the same ring-buffer + threading.Lock pattern
from `audit/log.py` for the policy stream. This keeps the codebase consistent
and the implementation is proven thread-safe.

**Stream vs enforcement independence**: The stream can be enabled independently
of IAM enforcement (`IAM_POLICY_STREAM=1`). When enforcement is off but the
stream is on, evaluations are recorded as Allow (since nothing is denied). This
lets developers see what would be evaluated without blocking requests.

**Suggest-policy groups by resource**: The least-privilege policy generator
groups actions by resource ARN, producing one Statement per unique resource.
This matches AWS Access Analyzer's output format.

**Wired into iam_middleware**: Rather than modifying the policy_engine itself
(which is a pure evaluation function), the recording hook lives in the
middleware layer where request context is available.

## Files changed

- `src/robotocore/services/iam/policy_stream.py` (new) -- PolicyStream class,
  singleton, helpers
- `src/robotocore/gateway/app.py` -- 4 new management endpoints
- `src/robotocore/gateway/iam_middleware.py` -- recording hook after each
  evaluation
- `tests/unit/services/iam/test_policy_stream.py` (new) -- 21 unit tests
- `tests/unit/services/iam/test_policy_stream_integration.py` (new) -- 8
  semantic integration tests
