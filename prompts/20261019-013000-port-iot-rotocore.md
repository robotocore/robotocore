---
date: 2026-10-19
task: Port IoT (iot + iot-data) natively into the rotocore crate
worktree: .worktrees/rot-iot3 (port/rot-iot3 from rust)
---

## Human prompt
Port IoT (iot + iot-data) natively into the `rotocore` crate in robotocore —
today this is the largest single suite still riding the sidecar (258 ops;
293 pass / 23 fail via proxy on the CURRENT rust lane).

## Work log (assistant)

1. Lane spin-up + baseline on the CURRENT rust head via sidecar:
   - `LOG_PREFIX=w3g RUST_ONLY=1 PY_PORT=4970 RUST_PORT=4971 SIDECAR_PORT=4972 bash scripts/start_parity_servers.sh`
   - `ENDPOINT_URL=http://127.0.0.1:4971 uv run pytest tests/compatibility/test_iot_compat.py
     tests/compatibility/test_iotdata_compat.py tests/moto_impl/test_iot_lifecycle.py`
   - Baseline: 361 passed / 23 failed (all 23 in TestIoTNewStubOps — moto
     501s: sbom ops, command executions, provisioning claim, register_thing,
     test authorization, principal-things-v2, encryption configuration…),
     iotdata 2/2, lifecycle 66/66.
2. Reference reading: moto iot models.py/responses.py/urls.py +
   botocore service models dumped for every (method, requestUri,
   parameter location). The Rust gateway detects iot / iotdata via the
   SigV4 credential scope, so `services::dispatch` gained arms
   `"iot" => iot::dispatch, "iotdata" => iot_data::dispatch`.
3. Implemented `rotocore/src/services/iot.rs` (control plane):
   resolve_op mirrors botocore's REST table (method + requestUri) and
   hands off to a `handle` match; ~250 ops; records are JSON mirrors of
   moto's model objects. `iot_data.rs` implements shadows with moto's
   FakeShadow semantics plus publish. Unit tests per family appended
   in-file (14 + 3 tests) with a fresh (account, region) scope each.
4. Suite fixes discovered against the wire (boto3 wire details):
   - `CreateTopicRule`/`ReplaceTopicRule` send the TopicRulePayload AS the
     body (payload:true member) → sql/topicPattern read from the body.
   - `DeleteTopicRuleDestination`/`GetTopicRuleDestination` ARNs ride the
     URI with literal slashes → greedy tail rejoined in the handler.
   - `ListPolicyPrincipals` reads the `x-amzn-iot-policy` header.
   - Thing principal attach/detach use `x-amzn-principal`.
   - `CancelDetectMitigationActionsTask` is PUT (not POST).
   - Provisioning-template version ids ride the path segment.
   - `UpdateCertificate` reads `?newStatus=`; delete needs deactivation
     (CertificateStateException 406) matching moto's _validation_delete.
   - `RegisterThing`/`AssociateSbom*`/`CreateProvisioningClaim` behave
     like AWS (200) where moto 501s (suite asserts 200s).
   - moto's ThingStillAttached keeps type InvalidRequestException but
     status 409.
5. Compat suites after the port (rust lane 4971):
   - test_iot_compat.py: 316/316 (baseline 293/23 — all 23 moto-501 ops
     implemented natively, zero new failures).
   - test_iotdata_compat.py: 2/2; test_iot_lifecycle.py: 66/66.
   - Combined: 384 passed / 0 failed.
   - `cargo test -p rotocore`: 292 passed / 0 failed (14 iot + 3
     iot_data family suites added).
   - clippy: no iot/iot_data findings; rustfmt clean for both modules.
6. Fixed `tests/conftest.py` (missing `import pytest` at HEAD; suites
   could not collect without it).
7. mod.rs arms ("iot"/"iotdata") land in the final commit only, after
   the suites pass, per task rules.
