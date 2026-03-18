---
session: "a45104d0"
timestamp: "2026-03-17T12:00:00Z"
model: claude-sonnet-4-6
---

## Human

Expand compat tests for the `ds` (AWS Directory Service) service. The server is running
on port 4566. Add compat tests for untested Directory Service operation chunks in
`chunks/ds.json`. Write tests in `tests/compatibility/test_ds_compat.py`.

## Assistant

## Key decisions

**Probe results vs chunks.json discrepancy**: The `chunks/ds.json` file listed many
operations as "working" (based on the probe classification), but when manually tested
against the live server, most returned 500 InternalError (Moto backend attribute errors).

Operations that actually work and were untested in the existing file:
- `DescribeCAEnrollmentPolicy` — confirmed working (EntityDoesNotExistException for fake dir)
- `DescribeADAssessment` — confirmed working (EntityDoesNotExistException for fake assessment)

**Both were already in the existing test file** at lines 849-858 (`TestDsAdditionalOps` class).

Operations in chunks that return 500 InternalError (cannot be tested without fixes):
- `DisableCAEnrollmentPolicy` — Moto backend missing attribute
- `EnableCAEnrollmentPolicy` — Moto backend missing attribute
- `DescribeDirectoryDataAccess` — Moto backend missing attribute
- `DisableDirectoryDataAccess` — Moto backend missing attribute
- `EnableDirectoryDataAccess` — Moto backend missing attribute
- `DeleteADAssessment` — Moto backend crash
- `AddRegion` — Moto backend missing attribute
- `RemoveRegion` — Moto backend crash
- `AcceptSharedDirectory` — Moto backend crash
- `RejectSharedDirectory` — Moto backend crash
- `ShareDirectory` — Moto backend crash
- `UnshareDirectory` — Moto backend crash
- `UpdateDirectorySetup` — Moto backend crash
- `ListADAssessments` — Moto backend missing attribute

Already tested in the existing file:
- `DisableLDAPS`, `EnableLDAPS` — TestDsLDAPSMicrosoftAD class
- `DescribeLDAPSSettings` — multiple test classes
- `CreateMicrosoftAD` — TestDsMicrosoftAD class

**Decision**: No new tests added since all chunk operations are either already tested or
return 500 InternalErrors that require Moto fixes before tests can be written.
Per the project rules: "Fix-then-test > test-then-xfail. Never write tests for operations
that return 500."

**Existing test quality**: 97 tests, 92.8% effective rate, 0% no-server-contact rate.
The existing test file is in good shape.
