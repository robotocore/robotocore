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

## Rerun (2026-11-08, tranche 16) — 7 stacks fail / 1 of 8 passes, triage

`make drift` after the #443-#445 wave: vpc_network + cicd_pipeline +
monitoring + auth_stack + data_lake + event_pipeline + rest_api fail;
`static_website` is the one passing STACK (the pytest row totals 7
failed / 2 passed because the corpus is 8 stacks + the
`test_stacks_exist` meta-guard). The corpus confirms the SAME open
classes as above, updated by the review's wire evidence. A SECOND
rerun after the SG-egress shape fix (#448 follow-up) keeps the same
7-failed census but EVERY SG-carrying stack now progresses past the
revoke phase — the failures shift to later resources (the vpc stack
hangs at route-table associations).

- **The vpc_network failure is rule-SHAPE drift, not a missing rule**:
  a fresh twin SG DOES materialize the default all-egress rule, but it
  carries an empty `IpRanges: []` where moto and real AWS record
  `[{CidrIp: 0.0.0.0/0}]` — so the provider's revoke of the
  `(IpProtocol=-1, no-ip-range)` rule answers `InvalidPermission.NotFound`
  (400 on the twin, 200 OK on moto, live-probed on identical
  sg-create).

  **FIXED (2026-11-08, verified live)**: create_security_group seeds
  the default rule with real AWS's full shape; the provider-style
  RevokeSecurityGroupEgress now removes it (200 → empty egress list),
  a SECOND revoke answers InvalidPermission.NotFound exactly like
  moto, and the wire row is parse-equivalent to moto's
  DescribeSecurityGroups (byte diff only in the twin's empty-element
  form: `<ipv6Ranges></ipv6Ranges>` vs moto's self-closing form — the
  renderer's own inconsistency is separate, pre-existing). Compat
  pin: TestSecurityGroupDefaultEgressShape. The
  SG phase of the vpc stack now passes; the stack subsequently hangs
  on apply at `aws_route_table_association.subnet_{a,b}` (Still
  creating → timeout — a NEW open lead: the twin never completes
  (or wrongly retries) the association create; TF_LOG wire session
  to capture the raw request/answer), so the vpc STACK still fails —
  at a later phase than before.
- Kinesis/Cognito `UnrecognizedClientException` unchanged (needs the
  same Go-SDK signing-region session).
- CloudWatch alarm readback + Cognito pool schema remainder unchanged.
