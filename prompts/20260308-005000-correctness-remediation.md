---
role: human
timestamp: "2026-03-08T00:50:00Z"
session: "d65b4916"
sequence: 1
---

Implement the correctness remediation plan: fix 6 confirmed bugs, audit wire
formats, deepen test coverage for all 38 native providers, build automated
parity verification, verify region isolation, and harden infrastructure.

Full plan provided as structured document with 6 phases, 9 execution steps.

---
role: assistant
timestamp: "2026-03-08T00:50:15Z"
session: "d65b4916"
sequence: 2
---

## Execution Summary

### Phase 1: Critical Bug Fixes (6 bugs) — COMPLETE
1. **1A**: Removed duplicate `request_counter.increment()` from `TracingMiddleware`
   (was also in `app.py:427`, causing every request to be counted twice)
2. **1B**: SNS→SQS delivery now parses queue region from endpoint ARN
   (`arn:aws:sqs:<region>:...`) instead of using the topic's region
3. **1C**: EventBridge→SQS delivery — same cross-region fix as 1B
4. **1D**: `error_normalizer()` now reads `jsonVersion` from botocore service
   metadata (via new `get_service_json_version()`) instead of hardcoding `1.0`
5. **1E**: XML-escaped all exception messages in error responses using
   `xml.sax.saxutils.escape()` in both `handlers.py` and `moto_bridge.py`
6. **1F**: Account ID validation changed from `parts[0].isdigit()` to
   `len(parts[0]) == 12 and parts[0].isdigit()`

Also removed dead code: `_extract_region()` and `_REGION_RE` from moto_bridge.py.

### Phase 2: Wire Format Audit — COMPLETE
- **2A**: Created `scripts/audit_json_versions.py`. All 44 hardcoded JSON
  versions in source match botocore metadata (0 mismatches).
- **2B**: Ran `check_wire_format.py`. Only known issue: apigateway v1 lacks
  camelCase conversion (larger fix, not in scope).
- **2C**: 14 error format consistency tests covering JSON, REST-JSON, query,
  REST-XML, and EC2 protocols. Special character handling verified.

### Phase 3: Test Depth — IN PROGRESS
- **3A**: Error-path tests delegated to worktree agent
- **3B**: Cross-region integration tests written for SNS→SQS and EventBridge→SQS
- **3C**: Pagination tests delegated to worktree agent

### Phase 4: LocalStack Parity Proof — PARTIAL
- **4A**: Created `scripts/parity_test.py` harness (8 services, 20+ operations)
- **4B/4C**: Existing `generate_parity_report.py` already covers operation coverage

### Phase 5: Store Isolation — COMPLETE
- **5B**: 11 region isolation tests for 10 services (SQS, SNS, Events, Lambda,
  SecretsManager, Kinesis, Scheduler, CloudWatch, StepFunctions)

### Phase 6: Infrastructure Hardening — MOSTLY COMPLETE
- **6A**: Removed dead `_extract_region` and `_REGION_RE` from moto_bridge.py
- **6B**: BaseHTTPMiddleware noted as low-risk deprecation (not migrated)
- **6C**: 16 input validation tests for all internal endpoints

### Test counts
- Started at: 2529 unit tests
- After all changes: 2570+ unit tests (41+ new)
- 0 failures, 0 xfails
