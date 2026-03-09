---
session: "7092342e"
timestamp: "2026-03-08T04:00:00Z"
model: claude-opus-4-6
---

## Human

Continue fixing CFN resource handler bugs surfaced by IaC tests. Previous session created ~245 IaC tests and found 27 server bugs. This session fixes those bugs to get tests passing. Also: are IaC tests in CI?

## Assistant

### Phase 1: CFN Engine YAML Parsing Fix (commit 0eba46ad6)
- Registered `yaml.add_multi_constructor` for CFN shorthand tags (!Ref, !Sub, !GetAtt, etc.)
- Fixed Lambda `!Ref` to return function name (not ARN)
- Fixed S3 website config (XML bytes) and tags (dict format)
- Added CFN parameter AllowedValues validation
- Added rollback on create failure (ROLLBACK_COMPLETE + resource cleanup)
- Added no-changes update detection

### Phase 2: Native Provider Store Fixes (commit 3a1d75597)
Key insight: CFN resource handlers were creating resources in Moto backends, but API calls route through native providers with separate in-memory stores. Resources created via CFN were invisible to API calls.

Fixed by creating resources directly in native provider stores:
- **Cognito UserPool/Client**: native `_get_cognito_store()` instead of Moto
- **Kinesis streams**: native `KinesisStore.create_stream()` instead of Moto
- **Lambda ESM**: native `_esm_store` instead of Moto
- **API Gateway**: `put_method`/`put_integration` (correct Moto API names, not `create_method`)
- **IAM Policy**: `put_role_policy` for inline policies (not `create_policy`)
- **CloudWatch Alarm**: removed unsupported Moto params (metrics, rule, unit, etc.)
- **Security Group**: apply ingress rules via `authorize_security_group_ingress` with positional args
- **Log Group**: apply RetentionInDays via `put_retention_policy`

### Phase 3: CI Addition (commit 3a1d75597)
Added `iac-tests` job to `.github/workflows/ci.yml`:
- Starts robotocore server
- Runs CFN, SAM, and Serverless IaC tests (no external tool binaries needed)
- Added as dependency for docker-build gate

### Results
- CFN IaC tests: 54/59 passing (was 32/59)
- Remaining 5 failures: nested stacks child resources, data lake cleanup, S3 PutObject 'str' decode bug (3 tests)
- Full IaC suite: 125 passed, 32 failed, 34 skipped (TF/CDK/Pulumi need tool binaries)

### Decision: Native Store vs Moto for CFN
When a service has a native provider (Cognito, Kinesis, Lambda ESM), CFN resource handlers MUST create resources in the native provider's store, not Moto's backend. Otherwise resources are invisible to API calls that route through the native provider. This is a fundamental architectural constraint.
