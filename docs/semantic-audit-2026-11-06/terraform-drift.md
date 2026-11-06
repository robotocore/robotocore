# Terraform drift oracle findings (2026-11-06)

The drift oracle (`tests/iac/terraform/test_drift.py`): apply each stack to
the twin, then `terraform plan -detailed-exitcode` must report no changes.
The AWS provider's refresh logic is the oracle — it names the exact field.

## Fixed by this sweep

- **DynamoDB `CreationDateTime` rendered as a datetime string** — real AWS
  sends a JSON NUMBER (epoch seconds); the terraform provider's decoder
  failed on every CreateTable/DescribeTable response ("expected Date to be
  a JSON Number, got string instead"). Now rendered as a number.
- **SNS topics without a `Policy` attribute** — real AWS always carries an
  auto-generated default topic policy (moto models.py:200-241); the provider
  unmarshals `attrs["policy"]` unconditionally and died with "parsing
  policy: unexpected end of JSON input". The default policy is now emitted.
- Harness-side: per-stack account id + auto-suffixed variable defaults
  (static name defaults collided across stacks/runs), whole-stack copy for
  `archive_file` data sources.

## Open (each reproducible by running the stack)

- **Kinesis/Cognito via the terraform provider answer 400
  `UnrecognizedClientException` ("The security token included in the
  request is invalid")** — not producible with boto3 (signed the same way:
  plain creds, session token both work) and the string exists in neither
  the twin nor moto; the provider logs `tf_aws.signing_region=""`. Needs a
  TF_LOG=DEBUG Go-SDK-side session to see the raw 400 body the twin sends
  (repro: `terraform apply` on tests/iac/terraform/data_lake or
  monitoring against the twin; the DynamoDB date fix above already
  un-blocks the catalog table).
- **vpc_network: `RevokeSecurityGroupEgress` answers
  `InvalidPermission.NotFound` for the DEFAULT egress rule** — real AWS
  gives every fresh SG a default all-egress rule; the provider revokes it
  when inline egress blocks exist. The twin's fresh SGs lack it (or their
  description differs so the revoke doesn't match).
- **CloudWatch `PutMetricAlarm`/refresh diverges** (monitoring stack) —
  plan-after-apply shows changes on the alarm resource; needs the
  alarm-response member diff (GetMetricAlarm-style readback).
- **Cognito User Pool schema attributes** drift on refresh
  (`min_length`/`mutable`/password-policy members — the twin's
  readback omits or reshapes them).

Run: `make drift` (after the twin is up).
