---
role: assistant
timestamp: "2026-03-07T22:05:00Z"
session: "cleanup-gap-analysis"
sequence: 3
---

## Smoke test expansion: 30 -> 94 tests

Extended `scripts/smoke_test.py` from 30 to 94 passing probe tests.

### Approach
- Kept existing 30 detailed functional tests as "Core service tests"
- Added `BULK_PROBE_TESTS` data-driven section for 64 additional services
- Each bulk test makes a single list/describe call with zero required params
- Used `botocore.session` to discover zero-param list operations for each service

### Findings
21 services initially failed. Root causes:
- **9 services with routing broken (501)**: appconfig, bedrock, macie2, managedblockchain, medialive, securityhub, signer, timestream-query, timestream-write
- **7 services with unimplemented Moto ops**: codecommit, directconnect, redshiftdata, sdb, servicequotas, textract, transfer
- **5 fixed by using different operations**: codebuild (list_projects), dms (describe_endpoints), memorydb (describe_clusters), cloudformation (list_stacks), shield (list_protections)

Also fixed Organizations test to be idempotent (catches AlreadyInOrganizationException).

Final: 94/94 passing.
