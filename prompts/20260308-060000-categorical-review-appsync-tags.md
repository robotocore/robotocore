---
session: "a09a1d51"
timestamp: "2026-03-08T06:00:00Z"
model: claude-opus-4-6
---

## Human

Find one part of our product that you believe the overnight script implemented
extremely well and review it. For every problem found, design a system-wide
categorical set of unit tests for it that can catch the whole category of
failures. When those tests fail, implement the fix.

## Assistant

## Key decisions

**Selected AppSync Event APIs v2** for deep review — 843 lines of provider code
with both v1 GraphQL and v2 Event API CRUD, the most substantial overnight addition.

**Categorical bug found: Tag stubs in REST-path native providers**

Three providers had tag endpoints that always returned empty, despite resources
storing tags during creation:
- AppSync: `GET /v1/tags/*` -> `{"tags": {}}`
- API Gateway V2: `GET /v2/tags/*` -> `{"tags": {}}`
- Scheduler: `GET /tags/*` -> `{"Tags": []}`

**Secondary bug: Recursive key case conversion corrupts user data**

apigatewayv2's `_pascal_keys()` and `_camel_keys()` recursively converted ALL
dict keys including user-provided tag keys. `{"tags": {"my-key": "val"}}` became
`{"Tags": {"MyKey": "val"}}` in storage, making lookups fail.

**Fix:** Added `_PASSTHROUGH_KEYS` set (Tags, StageVariables, etc.) that prevents
recursive case conversion of user-data values.

**Tests:** 13 categorical tests in `test_tag_consistency.py` covering all 3 providers.
All 13 failed before fix, all 13 pass after. 0 regressions across 3179 unit tests.

**Files changed:**
- `src/robotocore/services/appsync/provider.py` — real tag CRUD with ARN lookup
- `src/robotocore/services/apigatewayv2/provider.py` — tag CRUD + passthrough keys
- `src/robotocore/services/scheduler/provider.py` — tag storage + CRUD
- `tests/unit/services/test_tag_consistency.py` — 13 new categorical tests
- `tests/unit/services/test_appsync_provider.py` — updated stale test expectation
