# Moto-parity port — progress ledger

The goal contract lives in `docs/moto-parity-goal.md`. This ledger records
state so any resumed session (or reviewer) can verify progress without
trusting memory. One row per wave; `CLOSED <svc>` and `PARKED <svc>` lines
are parsed by `tooling/moto_port_progress.py`.

## Start state (2026-10-19, SHA 391b54bc)

- Old served surface (`1e9a1c37` registry, status `moto` or `native`): 157 services
- Non-native on the live twin: 65
  - 54 moto-backed in the old tree
  - 4 python-native providers that regressed in the port (`rdsdata`, `route53`, `ses`, `synthetics`)
  - 8 never-registered/moto-coverage-unproven (G2 set): `applicationautoscaling`,
    `dynamodbstreams`, `kinesis-video-archived-media`, `sagemaker-runtime`,
    `scheduler`, `servicecatalogappregistry`, `servicediscovery`, `neptune`
- Gates at start: 860 workspace unit tests pass; `make parity` 100% on 89
  promoted suites (drift-clean); clippy/fmt clean.
- Wider pytest lane baseline: `9254 passed / 168 failed / 17 errors / 69
  skipped` — the failures are the documented port backlog
  (`tooling/parity-known-artifacts.md`).

## Wave 1 (2026-10-19) — signer, shield, datapipeline, ram

- Ported: `signer` (26/26), `shield` (35/35), `datapipeline` (23/23), `ram` (41/41).
  Suites restored from `7170f904^`, all green against the merged twin on 4566.
- Superseded-arch note: an orphaned moto-proxy server from the abandoned
  `.worktrees/s3` worktree occupied ports 4641/4642 and was killed during
  the wave (superseded architecture, not the 4566 twin).
- G2: no tests removed — probes show 0 `not_implemented` for all four.
  datapipeline's 11 probe `500_error` entries were moto `KeyError` crashes
  on missing pipelines; implemented as real handlers answering the fork's
  `PipelineNotFoundException` (400) instead of reproducing the crash.
- Deliberate quirk retained: shield `DescribeProtection` with no selector
  answers moto's 500 InternalError (probe + unit test document it).
- signer suite: one test's hardcoded `localhost:4566` now reads
  `ENDPOINT_URL` (assertion untouched, needed to test against a worktree
  port).
- Registry rows flipped `moto`→`native` for the four; gates: 906 workspace
  lib tests, fmt/clippy clean, parity promoted (full lane drift-clean).
- Post-wave per gate script: REMAINING 54, G2 UNCERTAIN 8 (unchanged).

## Wave 3 (2026-10-19, on top of d40bc788) — twelve services

- Ported: `dms` (178/178), `quicksight` (242/242), `pinpoint` (136/136),
  `networkmanager` (112/112), `route53` (110/110 — one of the four
  python-native regressions closed), `ses` (92 of 113; 21 assertions for
  ops absent from the moto fork v1 surface removed with double evidence),
  `synthetics` (37/37, second regression closed), `elasticbeanstalk`
  (61/61), `memorydb` (36/36 with 15 moto-absent stub tests removed),
  `fsx` (48/48 of 63; 15 moto-absent removed), `panorama` (46/46),
  `connectcampaigns` (17/17; 8 moto-absent removed).
- **Regression note**: `route53`, `ses`, `synthetics` were python-era
  native providers that the first rust port had dropped; all three are
  native again here (rdsdata remains in the queue).
- Probe-staleness applied: dms/pinpoint/networkmanager/route53/ses/synth
  etics/elasticbeanstalk/panorama/quicksight implemented ops beyond their
  stale probes per fork source; agent-tracked test removals carry
  double evidence (probe row + absent handler in fork urls.py+responses).
- Cross-service fidelity fixes folded at merge (documented): NM agent
  added `transitGatewayArn` to the EC2 tgw_fields rendering (moto
  ClassPrefixAlias behavior; ec2 suite spot-checked green); ses agent's
  sesv2 `CreateContact` stores ContactListName + timestamps; memorydb
  seeds its default subnet group from the EC2 crate state — the first
  moto-style cross-crate dependency reuse.
- Infra: parity harness now always starts its twin with the repo venv
  python (`ROBOTOCORE_PY`), fixing deterministic lambda->service
  integration tests (they passed/vanished depending on which shell
  restarted the twin; root cause of the lambda_integrations 11->4
  drift). Promotion: exit 0 with 142 suites drift-gated,
  `lambda_integrations` preserved at its historical 11.
- Gates: 1136 workspace lib tests, fmt/clippy `--all-targets` clean,
  12 suites green vs the main twin (1115 passed in one process),
  promotion exit 0.
- Standing perf agents (4th-6th): disk held >=30G free through the wave;
  proposals (not applied): release strip=symbols and thin-LTO — parent
  may A/B with build-time trade-off.
- Gate after wave: 118 native of 157; REMAINING 39 = 31 to port + 8
  G2-uncertain.

## Wave 2 (2026-10-19, through SHA 04b99c03 + reconciliation/perf pieces) — ten services

- Ported: `ce` (61/61), `glacier` (38/38, re-armed an inert wave-3 leftover
  module), `workspaces` (97/97), `bedrock` (132/132), `wafv2` (76/76),
  `servicecatalog` (104/104 after reconcile), `s3control` (162/162),
  `textract` (44/44 after reconcile), `mq` (27/27), `mediastore` (29/29).
- **Probe-staleness ruling** (recorded by parent, verified by wave-2 review):
  probes were captured 2026-03-19, one day BEFORE the deployed moto fork
  gained surfaces (JackDanger/moto@robotocore/all-fixes `2016563fd`;
  uv-cache fork and archives are sha256-identical). Evidence per service:
  - mq: probes/mq.json marks 8 ops `not_implemented` while
    `moto/mq/responses.py:192-249` has all handlers → implemented per fork.
  - mediastore: probes mark 8 + 2 `needs_params`, but moto commit
    09ec12906 (2026-03-20) added the 10 missing MediaStore ops → all 21
    botocore ops implemented 1:1 with `moto/mediastore/responses.py:16-141`.
  - s3control: probes mark the tag trio 501 while
    `moto/s3control/responses.py:358/363/373` implements them → implemented;
    the 14 genuinely-absent ops stay `501 NotImplemented`.
  - textract: probes mark 19 ops 501 while the fork's urls.py routes the
    whole service and responses.py handlers cover 25/25 → reconcile commit
    `b2c9b9ad` re-implemented all, suite restored to 44/44.
  - servicecatalog: probes mark the 3 org-access ops 501 while fork commit
    065b86c15 provides them → reconcile commit `4fa24056` restored 104/104
    (state default DISABLED, per fork).
- **G2 deletion (kept):** wafv2 `GetTopPathStatisticsByTraffic` — zero
  occurrences in the fork checkout and moto 5.2.4 → the single suite test
  was removed with evidence (the only assertion deletion this wave).
- Deliberate shapes: mq normalizes moto IndexError-500 paths to the modeled
  `NotFoundException` 404 (only 501 means absent); workspaces mirrors moto's
  KeyError-500 shapes; s3control empty-body paths keep moto's 500
  InternalError; bedrock adds ARN resolution for job lookups; servicecatalog
  keeps fork's DISABLED default; shield's wave-1 500 quirk still documented.
- Infra: parity harness endpoint moved to `localhost` (AccountId subdomain
  APIs like s3control cannot use an IP endpoint — fixed the promotion hang
  and the phantom `pass=1/28` record); `--services` default now derives
  from suites on disk (129 gated); drift-tuple unpack bug fixed so real
  drift surfaces as exit 2; compat conftest stays on `localhost` (reverted
  a 127.0.0.1 "harden" that broke subdomain routing).
- Perf lane merged (port/perf2): OnceLock memoization of gateway native
  service names in server.rs — `/_robotocore/{health,services}` −39%
  (1.089→0.661 ms / 1.130→0.685 ms), byte-identical payloads, sqs suite
  green before/after. Standing perf agents have reclaimed disk to ~66G free.
- Promotion: full lane exit 0 on the warm twin; 129 suites drift-clean.
  (First fresh-twin attempt had a one-off apigateway 128/129 cold-start
  flake — isolation-greens; next rerun clean.)
- G4 lane count: 10160 passed / 157 failed / 17 errors (start row: 9254).
- Gate after wave: old served surface 157, native 106, REMAINING 51
  (43 to port + 8 G2-uncertain).

## Wave 4 (2026-10-19, on top of 377babe9; includes port/perf7) — twelve services

- Ported: `account` (16/16; 3 double-absent tests removed), `appconfig`
  (48/48), `autoscaling` (70/70; live EC2 loopback integration), 
  `bedrock-runtime` (2/2, InvokeModel 1:1), `bedrockagent` (2/2; 11 of 70
  fork-routed ops — the rest 501 exactly like moto), `clouddirectory`
  (72/72), `codebuild` (80/80), `codecommit` (121/121; sha1 crate dep
  added for moto's blob ids), `codedeploy` (41/41), `cloudhsmv2` (23/23),
  `dax` (26/26), `directconnect` (10/10; 50 double-absent of 60).
- **bedrock signing-scope finding**: bedrock-runtime and bedrock-agent
  sign with scope `bedrock`, so the twin sees service `bedrock` for all
  three. The `bedrock` arm chains
  bedrockagent -> bedrock_runtime -> bedrock by disjoint URI families;
  exact-name arms kept for literal clients.
- directconnect: botocore maps `OvertureService` onto the legacy
  `support` arm; the support arm now branches on operation (surfaces
  disjoint) instead of a router edit.
- Registry: 12 wave-4 rows flipped; `rdsdata`'s stale `native` label
  corrected to `absent` (queued, not ported).
- Wave-3 review minors folded: sesv2 CreateContact overwrites by email;
  UpdateContact refreshes LastUpdatedTimestamp; ec2 default VPC seeds
  one subnet per AZ with moto's Zone-id table (us-east-1 real mapping).
- Perf (port/perf7) merged: `strip = "symbols"` — binary −20.9% (45.0 to
  35.6 MiB), startup unchanged, round-trips verified. Commit `cd27fdd0`.
- Infra: union-merge fused the budgets arm into a comment (budgets 0/29
  for one promotion; unfused, 29/29); disk hit 100% under twelve
  concurrent worktree targets — reclaimed to ~67G free.
- Gates: 1245 workspace lib tests; 13 suites green vs main (643 in one
  process). Promotion exit 0, 153 suites drift-gated.
- G4 lane count (post-wave): 11793 passed / 150 failed / 17 errors
  (start row: 9254 — +2539 net passes since the goal opened; the
  remaining failures are the lambda-runtime backlog suites).
- Gate after wave: 130 native of 157; REMAINING 27 = 19 to port + 8
  G2-uncertain.

## Wave 5 (2026-10-19, on top of ce16a0b4) — twelve services

- Ported: `s3tables` (50/50), `ds` (124/124, 4 double-absent ops stay 501;
  EC2 interop per moto: subnets/AZs validated, security-group + ENI per
  subnet created in the native ec2 state, crashed fork ops implemented per
  probe messages), `s3vectors` (22 of 26; tags triple + QueryVectors
  unrouted in fork → removed with double evidence), `kinesisvideo`
  (30 of 38; 8 edge/media-storage ops unrouted → removed),
  `mediaconnect` (94/94), `transfer` (129/129), `swf` (40/40; three
  suite-invisible simplifications documented: no 1s empty long-poll
  sleep, timers tracked not fired, closeStatusFilter quirk verbatim),
  `service-quotas` (4 of 26; moto implements exactly 2 ops — rest 501
  verbatim; 22 removed), `sdb` (3 of 10; 7 removed — moto sdb surface is
  5 ops), `vpclattice` (8/8), `workspacesweb` (13/13), `route53domains`
  (36/36; moto's DurationInYears>=2 boundary quirk reproduced with a
  strictly-later second clock read).
- **sdb gateway fix (required)**: sdb is SigV2 — the signature rides the
  form body, so no Authorization header existed and the gateway could not
  name the service. src/core/server.rs gained a body_is_sdb() branch on
  sdb's version marker (2009-04-15, unique botocore-wide) — wave-5 sdb
  agent authored it; verify by running the sdb suite.
- Merge collateral fixed: duplicate arms/mod-dedup after union merges
  (sagemaker, cognito-idp arm, codepipeline/list dedupes); garbled bedrock
  comment lines removed; wave-4 review minors folded (autoscaling loopback
  now requires a Host header — no dead-port dials; directconnect suite
  docstring corrected to "9 of 11 handlers").
- Promotion: exit 0 with 135 suites drift-gated.
- Gate after wave: 142 native of 157; REMAINING 15 = 7 to port + 8
  G2-uncertain.

## Wave 6 (2026-10-20, on top of 70e3f59d) — four services

- Ported: `forecast` (23/23 — the fork implements only the DatasetGroup
  family; everything else 501 verbatim), `rdsdata` (8/8 — one routed op,
  ExecuteStatement default QueryResults), `servicecatalogappregistry`
  (3/3 — greedy `/applications/(.+)/resources` dispatch so ARN-addressed
  lookups resolve like moto's urls.py; ValidationException lookup by
  arn/id/name), `mediapackagev2` (30/30 — channel groups → channels →
  origin endpoints → harvest jobs; unix-seconds timestamps, tag-casing
  per botocore locationName, never_return InputType quirk).
- Infra: `POST /_robotocore/config` ported with the python twin's exact
  statuses (403 without `ENABLE_CONFIG_UPDATES=1`) — the
  iam-enforcement suite's 4 ERRORS become clean skips.
- Wave-5 review F2.1 fixed: `extract_account_from_request` now reads
  SigV2 `AWSAccessKeyId` from the form body (sdb per-account isolation;
  moto's get_account_id_from path).
- Gate after wave: 156 native of 157 (before appautoscaling/neptune).

## Wave 7 (2026-10-20, ten parallel worktree agents) — eleven services

- Ported: `medialive` (131/131; pagination tokens, batch-op failure
  shapes, per-kind transient states; ARN quirk kept), `lexv2models`
  (23/23; `$`-separated alias ARNs, auto-created `test` alias),
  `emrserverless` (11/11 with moto's re-sort-per-page paginator),
  `emrcontainers` (5/5; literal job-id regex preserved; 501 deny for
  modelled-but-unrouted ops), `mediastore-data` (7/7; real
  ContentLength, ETag literal, TEMPORAL storage class), 
  `sagemaker-runtime` (10/10; signing scope `sagemaker` chain; async
  invoke writes to a loopback mirror bucket), `scheduler` (34/34;
  checksum-token paginator, `x or "UTC"` timezone semantics),
  `servicediscovery` (34/34 including the lowercase `operationId` moto
  bug), `kinesis-video-archived-media` (10/10; fixed sample-micro
  payload, moto session-token shape), `dynamodbstreams` (20/20; 
  22-digit sequence numbers via u128 iterators, PartiQL INSERT/UPDATE/
  DELETE stream capture added to the dynamodb crate bridge),
  `applicationautoscaling` (9/9; 13 unit tests — ECS cross-service
  bridge kept: moto probes ecs_backends.describe_services on register;
  moto's enum fault order, namespace regexes, TargetTracking alarm
  fan-out shapes).
- G2 resolved by porting: `applicationautoscaling` and
  `dynamodbstreams` were the never-registered/unproven moto-coverage
  cases — both now native with probe+fork double evidence.
- `neptune` (G2 evidence case): moto implements neptune via the shared
  rds backend (moto/neptune/README + engine branches in rds/models.py:
  port 8182, `default.neptune1.3`, engine version 1.3.2.1) — the twin's
  rds backend gained the `neptune` label arm + MasterUsername retention
  fix; live probe 12/12 with a real neptune client; both `rds`-scope and
  `neptune`-scope label paths verified (botocore signs neptune under
  scope `rds`).
- Gate script after merges: 157/157 old-served services native;
  G2 list fully resolved; GATE PASS.
- Adversarial review of origin/rust..rust folded in: ECS probe separates
  moto's ClusterNotFoundException from the missing-service
  AWSValidationException; dynamodb stream capture skips serde copies on
  non-stream tables (notify stream-label keeps moto's naive-UTC shape);
  X-Robotocore-Account override ranked above the SigV2 body source;
  config endpoint echoes python str() reprs; four stale registry rows
  flipped (forecast, mediapackagev2, servicecatalogappregistry, rdsdata).
- Known gap (recorded): probes/*.json artifacts are not regenerated for
  any wave service (the sidecar-era prober is gone), so
  docs/coverage.html under-reports the rust fleet; coverage.html is a
  stale artifact awaiting a rust-side prober.
- Promotion: exit 0; baseline 180 suites drift-gated (wave-6/7 suites
  row appended with 358 combined passes).
- Infra: disk-full incidents twice (root at 100% from 12 concurrent
  worktree builds); reclaimed per the perf10 policy (merged-only
  worktree targets + main incremental).

## Wave 8 (2026-10-20) — the rest of the port: lambda engine clusters + admin + services

- **Lambda execution engine** (crates/lambda): nodejs executor (node
  bootstrap with embedded unzip; `Runtime.InvalidRuntime` when node is
  absent — behavior identical to the python twin's), custom runtimes
  (`provided.*`/`go` bootstraps with layers + env tables; stdin/stdout
  contract, Task.TimedOut semantics), invoke payload validation (400
  InvalidRequestContent), remaining-time countdown in the context shim.
  Suites: test_lambda_nodejs_compat 34/34, python edge cases 35/35,
  custom runtime 13/13.
- **Lambda function URLs** (`src/core/furl_exec.rs`): v2.0-style HTTP
  invocation of /lambda-url/{id}/* with Cors overlay — 9/9. **ESM
  trigger engine** (`src/core/esm_engine.rs`): SQS/batch + Kinesis/DDB
  stream wiring with ReportBatchItemFailures and FilterCriteria — 8/8;
  integrations 12/12 (VPC echo + engine env), event source 4/4. Also
  fixed `describe_table` to emit LatestStreamArn/LatestStreamLabel.
- **Lambda destinations + DLQ delivery** (async invoke record fan-out to
  SNS/SQS/EventBridge + DeadLetterConfig) — agent in flight.
- **CloudFormation mirror**: AWS::SQS::Queue properties now propagate
  (Attributes form vs JSON protocol) — 3 cfn_e2e flows green on the
  merged twin; organizations arm casing fixed for the three
  AWS*ServiceAccess ops (60/60).
- **Chaos admin API** ported from the python-era history: rules CRUD +
  interception hook in the catch-all handler; suite restored from the
  deleted lane file.
- **Cross-cutting error envelopes**: SQS/CloudWatch per-protocol error
  envelopes (XML for query, `__type` JSON for json protocol; 400
  statuses), S3 ListObjects 404 + BucketName in NoSuchBucket paths,
  SNS notification wiring for S3 topic targets (process-global SNS
  state), DynamoDB CreateTable attribute-validation messages, Athena
  tagless CreateCapacityReservation (moto normalization), iot cancel +
  sagemaker ListMonitoringAlerts + pipes NamePrefix filter fixes, ec2
  DescribeInstances unknown-id faults + the three VPC-endpoint suites
  green via suite wiring fix (single-twin fixture URLs).
- **Gateway**: value-less S3 query keys survive the crate handoff
  (virtual-hosted directory-bucket calls no longer misroute).
- Fill-lane effect: 160 failing tests → 29 (all in the destinations
  cluster mid-flight at the time of scoring) with 12,844 passes.

## Wave 9 (2026-10-21) — review-driven engine hardening + moto-surface backlog + EC2 crash sweep

- Wave-8/9 adversarial review verdicts folded (no blockers): two majors
  fixed — ESM redelivery bounded (per-mapping consecutive-failure budget
  of 5, fingerprint-keyed, unparks on new records or mapping update; a
  permanently-failing mapping delivered exactly 5 check-ins then went
  dead silent, verified live) and the bisect frontier now deepens to
  batch size 1 (failed halves held in memory instead of re-queued as
  the whole batch; live drop pattern [4,2,2,1,1,1,1]); furl OPTIONS
  preflights hit options_response instead of invoking.
- EC2 crash cluster swept (297 probe 500_errors → 0 live): root cause
  was a nested RwLock deadlock — CopyImage/CopySnapshot/CreateVolume
  reacquired the shared EC2 map read-lock inside the with_state write
  lease, hanging every later EC2 request (one such call wedged the
  probe twin's EC2 fleet-wide). Also 4 modeled-NotFound statuses
  corrected 404→400 and the DeleteDhcpOptions typo'd message no longer
  trips the prober's substring classifier. Live twin restarted on the
  fix and verified un-wedged. Triage artifact:
  tooling/ec2-crash-triage.json (129 moto-crash-faithful rows remain —
  moto 5.2.4 itself 500s on those auto-filled requests via its werkzeug
  page; oracle still at :4799).
- Moto-surface backlog (computed from the fresh probe corpus: 143 ops
  the fork implements that the twin 501s): mediaconnect was one
  percent-encoding binding bug (motorcords match raw segments;
  split_path now binds on the raw segment — 63 ops flipped, probes
  clean); ssoadmin +14 real ops (inline policies, managed policy
  attach/detach incl. moto's embedded aws_managed_policies dataset,
  provisioning records, UpdateInstance — 47 *genuinely* absent ops stay
  501); swf's 26 flagged rows were prober misclassification
  (UnknownResourceFault substring) — no gap; misc services: backup
  StartReportJob wiring + greedy recovery-point ARN paths (99 working),
  apigateway UpdateUsage on its real wire path + GetAccount null-body
  bug, acmpca policy/cert ops, cognito-idp auth/MFA ops,
  resource-groups tag-sync, timestreaminfluxdb, cfn
  DescribeStackRefactor; connect DeleteIntegrationAssociation proven
  501-faithful (fork never mounts it).
- Prober artifact truth: 500_error fleet-wide class now 0 (was 381),
  cloudtrail/config sink rows corrected as substring artifacts.
- Ledger addendum (correcting the wave-8 stale bullets): the prober is
  BACK (scripts/probe_service.py + scripts/lib/param_filler.py, budget
  + artifact envelope), all 158 probes regenerated, site rebuilt at
  79.表/10268 ops; parity promoted at wave-8 close; the old "prober
  gone / coverage stale" known-gap bullet is superseded by this row.
- Known artifact quirks (recorded, not fixed): the prober counts a
  ReadTimeout on a slow handler as 500_error (contract says crashed —
  consider a distinct timeout bucket); audit_out still records no error
  bodies (made the 400-flood diagnosis slower than it needed to be).
- Wave-9 gates: fmt clean, clippy silent, 1,605 workspace lib tests,
  full compat lane green (12,873+ passes), promotion exit 0.

## Wave 10 (2026-10-21) — the rest of the port: IaC lanes, shipped images, bindings

- **IaC lanes 181/220 → 220/220 (+1 nested-stack regression = 221)**.
  Root cause of all 37 original failures: the CFN engine minted
  placeholder ids without materializing. The CFN mirror now really
  creates resources: EC2 VPC family, S3 website/versioning/policy,
  Cognito UserPool/Client (real-AWS shapes; moto has no CFN hooks),
  CloudWatch alarms, API Gateway rest-api trees with integrations,
  Lambda inline ZipFile (real zips), nested stacks via S3 templates
  with outputs + cascade delete, true rollback semantics
  (unknown types fail the deploy ROLLBACK_COMPLETE).
- **Wave-10 adversarial review folded** (3 majors + minors): nested
  stacks unwind during parent rollback (child records reach
  DELETE_COMPLETE, child exports swept from ListExports like the
  parent's), mid-deploy child failures leave no recordless orphans,
  child-export importers are considered on delete-block, S3 mirror
  sub-PUT failures unwind loudly, IPv6 routes delete, InstanceTenancy
  forwards, Cognito ProviderName/ClientSecret GetAtt-coverable, GetAtt
  misses now fail loud like real CFN (no lane relied on the silent
  paths), moto's cfnresponse.py embedded in inline zips (stdlib urllib3
  fallback documented), furl host-form validates the region label (404
  on mismatch); regression test
  TestRollback.test_parent_rollback_deletes_nested_stack added.
- **Shipped images validated on the tip**: distroless main image
  (114MB, 23 layers, healthy, healthy smoke: SQS/S3 round-trip, 158
  services native, no shell/python) and the extension image (142MB,
  endpoints + proxy verified, python-free). No Dockerfile changes.
- **Language bindings fleshed out** (ts/go/rust now real; python
  unchanged): each a thin SDK helper (endpoint/account/session) with a
  runnable smoke against the twin — @aws-sdk v3 (ts, npm scripts),
  aws-sdk-clients pinned (rust standalone crate + examples/smoke.rs),
  aws-sdk-go-v2 (go test + env-gated live smoke). All live-verified SSH
  against the fleet at rest.
- **Cosmetics**: metadata.json + changelog now say 158 services;
  /_robotocore/config's specifications_dir answers "embedded" instead
  of a dead mac-homebrew path.
- **Repo hygiene**: stale wave worktrees pruned (106 → 2; branches
  retained; dirty content archived under /tmp/wt-backup).
- Wave-10 gates: fmt clean, clippy silent, 1,608 workspace lib tests,
  full compat lane green (12,873 passes), IaC battery 119/119 on the
  cfn/serverless/sam slices + 101 across cdk/pulumi/terraform.

## Perf profile (2026-10-21) — robotocore vs moto 5.2.4 (python)

A/B profiler landed in `tooling/perf-ab-2026-10-21/` (harness + raw
results). Same host, one harness process, boto3-only client timing,
400/200 ops per cell, then a 4-thread mixed cycle. Rust's serial p50
floor (~0.41 ms) is the boto3 client itself; the twin's own audit
`duration_ms` sits at 0.03–0.11 ms server-side for the same cells.
Throughput: rust holds 941 wire ops/s under 4 threads vs moto's 467
(2.0x), with moto's GC/serialization tails dominating at the slow end
(sqs.receive p95 12.9 ms vs 0.47; ec2.describe_instances p50 6.27 ms
vs 0.41, a 15x gap). RSS: 27.7 MB rust vs 220 MB moto under load —
~1/8 the memory. Only real regression the profiler surfaced: none —
`dynamodb.scan Limit=10` is the smallest win (2.4x) and would be a
candidate for the same collect_listing fix the standing perf agents
proposed (`crates/s3` list path analog on the dynamodb scan path).

## Wave 10 addendum (2026-10-21) — language bindings landed

The three SDK-helper bindings (ts / go / rust — writes under
bindings/{typescript,go,rust}/) were re-verified and merged to rust
after the perf sweep: typescript (robotocore.ts +
`npm run typecheck`/smoke), go module (`go vet` clean, skippable
live smoke), rust standalone crate (cargo build green on the merged
tree). Live-verified against the twin on their own scratch twins at
their creation; mirrors the python client's shape. That completes the
goal contract's bindings clause in full: python sits alongside
typescript, go, and rust — each a real, runnable client.

## Design (2026-10-22) — the botocore-surface plan (100% coverage + exercise tiers)

`docs/botocore-surface-plan.md` is the committed contract for taking the
fleet from moto parity (complete) to 100% botocore-surface coverage:
10,268 modeled ops today = 8,396 working / 649 needs_params /
1,163 moto-absent-not_implemented / 60 crash-faithful. Four machines
define it: coverage.py (the per-op surface artifact), probe filler
gen-2 (needs_params → 0), a botocore response-shape validator as the
wire-tier conformation gate, and an absence manifest that makes every
remaining deny a recorded decision. Exercise is defined as four screens
(probe conformance / unit / caller / system), with CI budget ratchets
wired through `make parity` and the parked workflow. Execution: ~8–15
waves of the established fleet pattern.

## Wave 11 (2026-10-22) — beyond-moto surface coverage: 1,163 → 485 not_implemented

- Fleet: covered 9,454 working ops of 10,268 (92%); probes refreshed
  under the two-phase filler (needs_params 649 → 266 via shape-driven
  synthesis: enum first, ARN/queue/bucket/token kinds, string
  min-length and integer min/max constraints honored from
  shape.metadata).
- Wave-11 clusters (agents per worktree, wave-8 pattern): **ec2** — 242
  remaining ops modeled through a generic shape-synth terminator
  engine (`SynthCtx`): 752/752 working; **connect** — all 148
  instance-family rows; **lexv2models** 85 + **personalize** 67;
  **networkfirewall** 61 + **bedrockagent** 61 + **forecast** 58.
- One regression fixed: redshift's `fetch_subnets` unit-test mirror
  dialed a dead hardcoded port → resolved-empty (wave-5 ASG rule);
  warning: foreign fleet twins on :4567/:4566 caused cross-talk.
- Budget ratchet: workspaces' four 500 rows (moto-KeyError-faithful,
  keyed by probe-directory-id strings) reclassified as
  crash-faithful-not-covered — the coverage gate caught the flip
  correctly and the ratchet file is regenerated at wave close with the
  reason documented here.
- Process hazards recorded: shared git-stash across worktrees popped
  foreign patches twice (both agents recovered; fleet rule is now
  commit-incrementally / no cross-worktree stashes); disk hit 100%
  twice mid-wave, agents honored the prune-and-wait discipline.
- Gates at close: fmt clean, clippy silent, 1,669 workspace lib tests,
  full compat lane green on the wave-11 twin, coverage gate ratcheted.

### Wave-11 review settle (2026-10-23, post-aa9ec804) — workspaces corpus truth

- The adversarial review of the wave-11 range found the committed
  workspaces probe row optimistic: `DeregisterWorkspaceDirectory`
  answered OK on the warm fleet twin only because of probe-order state
  dependence — an earlier op in the sweep had already seeded the
  probe-directory-id state moto's dict-index deregistration path reads.
  A faithful cold twin probes every one of moto 5.2.4's fields empty.
- Re-probed on a cold scratch twin: 83 working / 4x500 crash-faithful /
  4 needs_params (91 ops). The four 500 rows are the modeled moto
  `KeyError` crashes — moto `workspaces/models.py:889-892` does a bare
  dict index on the directory/resource/image id — for
  `DeregisterWorkspaceDirectory`, `ModifyClientProperties`,
  `ModifySelfservicePermissions`, `UpdateWorkspaceImagePermission`.
- Budget ratchet settled `workspaces` 9231 → 9121 (= round(83/91)):
  aa9ec804's 9231 counted the flaky OK row. No server-side change.
- Same review caught the wave-11 filler regression: connect's
  `CreateEvaluationForm`/`UpdateEvaluationForm` had slipped to
  needs_params; KNOWN_PARAMS seeds now fill them, connect 365 working
  of 366, ratchet 9918 → 9973. The probe classifier keeps client-side
  attribute gaps as needs_params and connection failures as timeout —
  500_error is reserved for genuine server-side error payloads.

## Wave 8 addendum (2026-10-22) — prober rewrite, corpus regeneration, backlog ports

The wave-8 close's tooling pieces, recorded here because the wave-9
row carried them only inline (the prober/corpus/site facts and the
backlog ports, per the fleet corpus):

- **Prober rewrite**: `scripts/probe_service.py` +
  `scripts/lib/param_filler.py` — rust-first, budget + artifact
  envelope; one `probes/<service>.json` per registered service
  (working / needs_params / not_implemented / 500_error / timeout).
  Supersedes the wave-7 "probes not regenerated" known gap.
- **Full corpus regeneration**: all 158 `probes/*.json` artifacts
  regenerated from the fleet sweep on the rust twin; the
  `tooling/coverage.json` + `tooling/coverage-budget.json` surface
  machine regenerates from that corpus under the per-service ratchet
  gate.
- **site/ rebuild**: `docs/coverage.html` + `site/` parity report
  rebuilt from the regenerated corpus; the stale sidecar-era
  dashboard is retired.
- **Backlog ports folded into the wave range**: `mediaconnect`
  (+63 ops — the percent-encoding binding fix un-gated them),
  `ssoadmin` (+14 real ops; 47 genuinely-absent ops stay 501),
  `swf` (the 26 flagged rows were prober misclassification, no gap).
