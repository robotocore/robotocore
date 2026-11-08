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
  pin: TestSecurityGroupDefaultEgressShape.

## Rerun after wave 2 (2026-11-08) — FOUR more drift root causes fixed, vpc_network PASSes

The TF_LOG wire session surfaced a chain, each fixed and live-verified:

1. **Route-table association rows carried no RouteTableId /
   AssociationState** — the provider's association waiter polls
   DescribeRouteTables and waits for `associationState.State` to reach
   "associated"; the bare association member hung every
   aws_route_table_association create for its full 5-minute wait.
   Fixed in route_table_item; pins:
   TestRouteTableAssociationShape.
2. ** moto's SecurityGroupRule drops the ports for protocol -1**
   (security_groups.py:60-66) — a revoke carrying FromPort/ToPort -1
   (the provider's default-revoke shape) must key-match the seeded
   port-less default rule; the twin's parse keeps -1/-1 and missed.
   parse_permissions now normalizes; pin:
   TestSecurityGroupAllProtocolRevoke.
3. **The VPC's main route table had no main association row** and
   DescribeRouteTables lacked the association.* filters — the
   provider's waiter polls Filter `association.route-table-
   association-id` (35 straight polls, all empty) and the
   aws_vpc waiter polls `association.main`+`vpc-id`. create_vpc now
   seeds the main association (moto's main_route_table_association)
   and describe_route_tables renders association.route-table-
   association-id / association.subnet-id / association.route-table-id
   arms.
4. **Create-path tag specifications were dropped** for IGW/route-table/
   security-group creates (wire_tags read only bare Tag.N) and
   **create-VPC defaulted enableDnsSupport=false** (real AWS defaults
   true) — every plan-after-apply fought Name-tag re-adds and the
   dns-support flip. tag_spec_tags now merges into the three creates
   (pins: TestVpcDefaultsMirrorRealAws), and the VPC create default
   mirrors real AWS.

Results: **vpc_network and static_website now PASS the drift oracle**
(apply completes and plan shows no changes); the six remaining stacks
all progress past SG/route-table phases and fail at their NEXT
resources (lambda, kinesis, etc. — per-stack leads queued).

## #452 review NITs resolved (2026-11-08)

- association.route-table-id now returns the table's OWN id even for a
  table with zero associations (moto parity,
  moto/ec2/models/route_tables.py:127-128, live-probed).
- main-assoc delete semantics: real AWS (and the twin) answer
  DependencyViolation when deleting the main (rtbassoc-carrying) route
  table while moto permits it — real-AWS-preferred, recorded + pinned.
- ModifySecurityGroupRules port normalization CHECKED against moto
  re-checked live against moto's modify source and wire: moto's
  modify path (models/security_groups.py:886-888, bypassing
  SecurityGroupRule.__init__) KEEPS
  FromPort/ToPort -1 on an all-protocol rule (the port-drop in
  SecurityGroupRule.__init__ is the CREATE path only; the seeded
  default rule is port-less because of its origin). The twin pins
  both row shapes (TestSecurityGroupRuleModifyPortNorm +
  TestModifyKeepsAllProtocolPortsDossier — the dossier's first draft
  collided with the seeded -1/0.0.0.0/0 rule and answered
  InvalidPermission.Duplicate on both engines; the shipped pin uses a
  distinct cidr).

## SG-row grouping + SecurityGroupRules view (2026-11-08, probed)

Two wire divergences surfaced while pinning the wave-2 rows, both fixed:

1. **moto folds same-(protocol, ports) permissions into ONE wire row** —
   two -1 cidr authorizations render as a single IpPermissionsEgress
   entry carrying ALL the CidrIps; the twin kept N rows. The SG describe
   render now folds rows in display_rows (the stored per-rule rows stay
   split so the sgr ids stay stable). Pin:
   TestSecurityGroupRowGroupingAndSgrView.
2. **The SecurityGroupRules view canonicalizes the all protocol to
   FromPort/ToPort -1** even for the port-less seeded default
   (live-probed) — the twin rendered the keys ABSENT. describe_sgr now
   emits -1/-1 for protocol -1. Known diff recorded: moto's sgr view
   has NO ip-protocol filter (group-id/security-group-rule-id/tags
   only; an unknown filter KeyErrors); the wave-2 draft briefly added
   one and the review caught it — dropped. Pin:
   TestSecurityGroupRowGroupingAndSgrView second test.
- Kinesis/Cognito `UnrecognizedClientException` unchanged (needs the
  same Go-SDK signing-region session).
- CloudWatch alarm readback + Cognito pool schema remainder unchanged.
