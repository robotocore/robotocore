---
role: assistant
timestamp: 2026-03-10T12:00:00Z
session: sfn-mock-config
sequence: 1
---

# StepFunctions Mock Configuration Support

## Task
Implement SFN_MOCK_CONFIG support for robotocore, matching LocalStack's mock configuration format for deterministic Step Functions testing.

## Approach
1. Created `mock_config.py` module for loading, parsing, validating, and hot-reloading JSON mock configs
2. Extended ASLExecutor with `mock_test_case` parameter that intercepts Task state execution
3. Integrated into provider.py: StartExecution and StartSyncExecution resolve mock test cases from execution name `#suffix` or `X-SFN-Mock-Config` header
4. Mock test case propagates to child executors (Parallel/Map branches)

## Key Design Decisions
- **Mock only Task states**: Pass/Wait/Choice/Succeed/Fail execute normally since they don't call external services
- **mtime-based hot reload**: Re-read config file when mtime changes, no filesystem watchers needed
- **Test case selection priority**: Header > name suffix > no mock
- **Clean execution name**: `#TestCase` suffix is stripped from the stored execution name/ARN

## Test Coverage
- 19 unit tests for mock_config.py (loading, parsing, lookup, hot-reload)
- 12 semantic tests for mock execution (Return, Throw, Parallel, history, output)
- All 48 stepfunctions unit tests pass (including 17 pre-existing)
