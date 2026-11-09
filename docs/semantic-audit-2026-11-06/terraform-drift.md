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
- **Kinesis/Cognito `UnrecognizedClientException` — RESOLVED
  (2026-11-08, TF_LOG=DEBUG wire capture): NOT a twin bug.** The drift
  harness's provider override lacked `cognitoidp` and `kinesis`
  endpoints (provider 5.100's key is `cognitoidp`, no underscore), so
  those requests hit the REAL global endpoints and the fake creds got
  real AWS's UnrecognizedClientException. With the endpoints added,
  auth_stack applies and plans clean end-to-end.
- **Cognito user-pool describe rendered no DeletionProtection** — the
  auth_stack plan then added `deletion_protection = "INACTIVE"`
  forever. real AWS always renders the member; moto omits it (known
  diff). The twin now renders the INACTIVE default; pin
  TestDeletionProtectionRender.
  Closed (2026-11-09): the describe-side default left the STORAGE half
  — create/update dropped the wire member (moto carries it as a
  whole-body pass-through into extended_config,
  cognitoidp/models.py:470-472, with no literal handling; its reset-on
  -update came from the same rebuild — an update that omits the member
  resets it, per botocore's UpdateUserPool doc "if you don't provide a
  value for an attribute, Amazon Cognito sets it to its default
  value"). So a terraform pool with `deletion_protection = ACTIVE`
  flipped back to INACTIVE on the next refresh. Create now stores;
  UpdateUserPool mutates and RESETS to the INACTIVE default when the
  caller omits the member — matching both oracles (preserve-on-omit
  was falsified by the adversarial reviewer's live moto probe).
  describe renders stored-or-default. 3 pins added to
  TestDeletionProtectionRender (create-store, flip → unrelated update
  → reset, INACTIVE write-back). Junk-enum values echo as stored (moto
  does the same pass-through); the real-AWS Delete-guard on an ACTIVE
  pool is implemented too — closed in the 2026-11-09 cognito Delete
  UserPool guard section below.
- CloudWatch alarm readback + Cognito pool schema remainder unchanged.


## Cross-service lambda runner ImportModuleError (2026-11-08, root cause)

28 cross-service tests failed with the handler import error naming the
HANDLER module — the runner's `python3` was the daemon's PATH python
(no boto3), and every user handler in the suite imports boto3. Fixed
two ways:

1. **find_python3 discovers the repo .venv python** (walks up from
   current_exe to .venv/bin/python3, matching moto's runner-owns-deps
   behavior); ROBOTOCORE_PY still wins when set.
2. **The import error names the REAL missing module** — "Cannot find
   module: boto3 (imported by <handler>)" instead of the misleading
   handler name — so dep gaps read like real AWS's ImportModuleError.

## monitoring stack: SNS DisplayName echo (2026-11-08, probed)

The plan's `-display_name -> null` was the twin's display_name()
FALLBACK to the topic NAME (crates/sns/models.rs) when the topic has no
DisplayName attribute — moto and real AWS echo an EMPTY DisplayName
instead. Fixed to unwrap_or_default; the drift stack now PASSES
end-to-end. Pins: TestDisplayNameEcho (unset echoes empty;
set-topic-attributes round trips).

## terraform kinesis resource: DescribeStreamSummary EncryptionType (2026-11-08)

The terraform kinesis resource refresh reads DescribeStreamSummary; the
twin's summary omitted `EncryptionType` outright (the raw describe
carried it after the sweep fix) while moto and real AWS always render
it (default NONE). Both describe shapes now share the stream's stored
value. Pin: TestDescribeStreamSummaryShape (summary == describe ==
NONE).

## event_pipeline stack: ListTags on the mapping ARN (2026-11-08, probed)

The drift apply died mid-run: the provider's lambda ESM refresh calls
ListTags on the mapping ARN
(`arn:aws:lambda:<region>:<account>:event-source-mapping:<uuid>`) and
the twin's ListTags routed the ARN through the function resolver —
"Function not found" — aborting the whole apply. moto resolves the ARN
member by uuid against its `_event_source_mappings` store
(awslambda/models.py:2447-2462) and stores create-time Tags
(models.py:1348). The twin now carries per-mapping tags behind a
RwLock (manual Clone like LambdaFunction), dispatches the three tag
ops on the ARN member, and 404s moto's message for an unknown uuid.
Pins: TestEventSourceMappingTags (5 routing/shape tests plus the
function tag path regression). event_pipeline drift now PASSES
(1 passed live, 45s — the SQS create's 25s provider poll included).

Follow-on from the same review pass: moto always renders `Tags` in the
ESM body dict (models.py:1429; `{}` survives the None-filter at
1433-1434) — the twin's esm_dict omitted the member. Rendered now;
(real AWS's documented response shape does not carry it — botocore's
model mirrors that and drops unmodeled members at parse time.)
The pin is the crate's own unit test
(esm_wire_dict_carries_tags_member) because botocore's
EventSourceMappingConfiguration has no `Tags` member and its parser
drops unmodeled members silently — no boto3-level test can see the
member on either side. The terraform ESM refresh reads tags via
ListTags, so no drift-stack surface changes either way.

Follow-on from the #460 adversarial review: the same refresh also sets
`kms_key_id` from the summary — moto renders the stored KeyId in BOTH
describe shapes (kinesis/models.py:449, 466), the twin hardcoded null
in the raw describe and omitted the member from the summary. A
KMS-encrypted stream therefore ran the phantom-drift loop again. Both
shapes now render the stored value (null ⇔ empty-string storage, the
ARN once StartStreamEncryption ran). Pin:
TestDescribeStreamSummaryShape.
test_key_id_renders_in_both_describe_shapes.
No drift stack in the repo sets `encryption_type`, so the oracle can't
drive this yet — the pin carries the live KMS round trip.

Postscript (2026-11-09) to the same lambda review pass: the ESM body's
`LastProcessingResult` was a twin-side phantom — moto stores the member
as `None` (awslambda/models.py:1407) and its None-filter (1433-1434)
drops it, so moto bodies NEVER render it, while the twin inserted
`"LastProcessingResult": null` on every body. Invisible through boto3
(parsers.py:705 skips null members) but wire-visible to Go-SDK
clients, and the esm_dict comment claimed moto-mirroring (backwards).
Dropped to moto parity; the dict-contract unit test pins the absence
(red pre-fix). Terraform surface unchanged: the provider DOES read
`last_processing_result` into a Computed attr
(terraform-provider-aws v5.100 stream: `d.Set("last_processing_result",
output.LastProcessingResult)`), but aws-sdk-go-v2 decodes JSON null and
absent into the same nil pointer, so the refreshed state is the empty
string either way and no plan diff can form.

## cognito: DeleteUserPool guard on a DeletionProtection=ACTIVE pool (2026-11-09)

real AWS refuses to delete a deletion-protected pool — botocore's
UserPoolType.DeletionProtection doc: "When you try to delete a
protected user pool in a DeleteUserPool API request, Amazon Cognito
returns an InvalidParameterException error. To delete a protected user
pool, send a new DeleteUserPool request after you deactivate deletion
protection in an UpdateUserPool API request." moto deletes
unconditionally (recorded known diff); the twin is real-AWS-preferred
on this surface since #456. The guard claims only an EXPLICIT stored
ACTIVE member (absent = the INACTIVE default) and the pool SURVIVES
the refused delete; the error message string is twin-authored (the
recorded oracle pins the code, not the text). Pins:
TestDeletionProtectionRender.DeleteGuardBlocksActivePool +
DeleteWithoutMemberStillWorks.

## lambda ESM optional configuration members (2026-11-09)

moto stores `StartingPositionTimestamp`, `ParallelizationFactor`,
`DestinationConfig`, `ScalingConfig` and `TumblingWindowInSeconds` via
`spec.get` (awslambda/models.py:1327-1350) and renders FOUR of them
through its None-filtered configuration dict (1413-1431 — the dict
has NO `StartingPositionTimestamp` key at all; raw-HTTP-verified by
the #467 review) — the twin dropped them on the floor. The terraform
`aws_lambda_event_source_mapping` provider schema carries
parallelization_factor and scaling_config as attributes, so mappings
configured with them phantom-re-planned forever (no drift stack sets
them, so the oracle never caught it — the #463 review's shape-gap
list). Create stores the wire values; update follows moto's elif loop
(models.py:2238-2260 — provided members mutate, omitted members
preserve); StartingPositionTimestamp is create-immutable (botocore
omits it from UpdateEventSourceMapping input; moto has no update
handler) and stored-but-never-rendered (moto-mirror; the real-AWS
render is a recorded model-derived divergence — no drift stack sets
it). Raw-only caveat (recorded, unreachable via boto3): an update that
carries an explicit null or a wrong type makes moto MUTATE to
None/raw (member vanishes or junk renders), while the twin preserves
the old value; the pre-existing FilterCriteria slot stores explicit
nulls verbatim — three behaviors, all SDK-invisible. Pins:
TestEventSourceMappingOptionalConfig (boto3-visible, unlike the Tags
member) and the crate's
esm_optional_config_members_store_render_and_update (the AT_TIMESTAMP
store-without-render + immutability case). MetricsConfig / Queues /
SourceAccessConfigurations / provisioned poller members stay parked.

## kinesis: PutRecord/PutRecords live EncryptionType (2026-11-09)

The KMS leg is capture-anchored: AWS's own put captures (CLI reference
put-record / put-records outputs on KMS streams) carry
`"EncryptionType": "KMS"` — the botocore model lists the member as
OPTIONAL in both outputs (required lists carry none of it), and
whether real AWS renders the member on a PLAIN stream is an open
real-AWS observation. The twin echoed the constant "NONE" — stale on
KMS streams once StartStreamEncryption ran (review lead O2 from the
#466-era kinesis pass, flagging that the constant made the put
surfaces half-honest next to the describe shapes). Both put responses
now render the stream's stored type (NONE after Stop). moto's put
responses OMIT the member entirely (kinesis/responses.py:108-123
put_record returns SequenceNumber+ShardId only) on plain AND KMS
streams (moto's describe still renders KMS — recorded asymmetry) — a
recorded known diff: the member follows the describe shapes' real-AWS
preference (and the standing presence pins). 2 compat pins tightened
to the live round trip (NONE → KMS → NONE, stop leg on both
surfaces).

## dynamodb batch limits: queue lead closed (2026-11-09)

The pre-#439 "batch limits (tranche 5b)" queue lead is already covered:
BatchGetItem mirrors moto's three-level validation (per-table >100 with
the exact provider message, the across-tables "Too many items requested
for the BatchGetItem call" cap, duplicate keys) — #439 landed the pins
and TestBatchGetItemLimits still carries them; live-probed against moto
5.2.5 this session, messages identical. BatchWriteItem has NO cap in
moto (live-probed: 26 PutRequests accepted, all stored) — the twin
mirrors it; the real-AWS >25 ValidationException stays a recorded
divergence (no drift stack batches writes) protected by
TestBatchWriteItemMirror.test_twenty_six_put_requests_accepted, which
fails if an un-mirrored cap is added later.
