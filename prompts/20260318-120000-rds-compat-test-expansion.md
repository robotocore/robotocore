---
session: "a3c7319e"
timestamp: "2026-03-18T12:00:00Z"
model: claude-sonnet-4-6
tools: [worktree]
sequence: 1
---

## Human

Expand compat tests for the **rds** service. Target the untested-but-working operations (gap=23).

Steps:
1. Run chunk_service.py to see which operations need tests
2. Verify each operation works against the live server before writing a test
3. Write tests in tests/compatibility/test_rds_compat.py
4. Follow quality rules: every test must contact server, assert on response field, use unique names

## Assistant

## Key decisions

**Probing first**: Used chunk_service.py and direct boto3 calls against localhost:4566 to classify all untested operations before writing any tests. Discovered that DescribeSourceRegions, DescribeDBClusterBacktracks, DescribeDBMajorEngineVersions, and DeleteDBClusterAutomatedBackup return NotImplemented — excluded them entirely.

**Three test class structure**: Split tests by pattern type:
1. `TestRDSDescribeListOperations` — 19 describe/list ops with no required params, assert response key exists
2. `TestRDSErrorResponseOperations` — 9 ops that require nonexistent resource IDs to verify correct error codes
3. `TestRDSParameterGroupCRUD` — 7 full CRUD tests for parameter groups and cluster parameter groups

**DeleteDBInstanceAutomatedBackup**: This returns HTTP 200 with a stub record (not an error) even for a fake resource ID. Included a test asserting the response contains "DBInstanceAutomatedBackup".

**Parameter group copy**: Verified that CopyDBParameterGroup requires an existing source group (it errors on "default.mysql8.0" despite that being the default), so tests create a source group first.

**DB subnet groups skipped for new tests**: Already covered by an existing fixture and tests. No need to duplicate.

Result: 35 new tests, 0% no-server-contact rate, ruff clean, all passing against port 4566.
