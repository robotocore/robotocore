---
session: "logs-scheduled-query-fix"
timestamp: "2026-03-15T00:00:00Z"
model: claude-sonnet-4-6
---

## Human

Fix failing test: TestLogsNewOps.test_create_scheduled_query_conflict
Failed: DID NOT RAISE ClientError (ConflictException)

## Assistant

## Key decisions

The test was written to exploit a bug (Moto read `queryName` but botocore sends `name`, resulting in `name=None` for all creates; second create would conflict with the first `None`-named query). The bug has since been fixed.

**Root cause**: 5 param-name mismatches in Moto's logs/responses.py for scheduled query operations:
- `create_scheduled_query`: queryName→name, logGroupNames→logGroupIdentifiers, targetConfiguration→destinationConfiguration, response wrapped wrong
- `get/update/delete/history`: arn→identifier (botocore uses `identifier`, not `arn`)
- `update_scheduled_query`: enabled (non-existent)→state

**Fix**: Corrected all param names in vendor/moto/moto/logs/responses.py. Also updated `create_scheduled_query` response to match botocore output shape `{scheduledQueryArn, state}` instead of `{scheduledQuery: {...}}`.

**Test fix**: Rewrote test to properly test the conflict: create a query first, then attempt a duplicate, assert ConflictException. Added cleanup in finally block.
