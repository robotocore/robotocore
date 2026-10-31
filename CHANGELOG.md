# Changelog

This file follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Robotocore releases historically use CalVer (`YYYY.M.D[.N]`) and now carry
semver majors; pushes to `main` auto-tag and publish versioned + `:latest`
Docker images. Each release gets a top-level section here; the project
source of truth for the maintenance policy is [`CLAUDE.md`](CLAUDE.md)
under *Changelog discipline*.

## Unreleased

### Fixed

- **S3 decodes `aws-chunked` upload bodies.** `PutObject`, `UploadPart` and
  replication stored streaming uploads as received, so objects kept the chunk
  headers, chunk signatures and checksum trailer. Bodies sent with
  `Content-Encoding: aws-chunked` or an `x-amz-content-sha256` of `STREAMING-*`
  (signed chunks, and unsigned chunks with a checksum trailer) are now decoded
  first; a body whose size disagrees with `x-amz-decoded-content-length` gets
  400 `IncompleteBody`. Affected the .NET SDK's default `PutObject` over plain
  HTTP and SDKs that stream checksummed uploads.

## 2.0.2

The v2.0.1 follow-through: the review waves against the merged 2.0 tree end
with kinesis mappings that bind their stream position at attach time, a
CloudWatch alarm engine that notifies through SNS and Lambda exactly like the
real service, runtime-family executors riding the published image, and CI
lanes whose gates read the tree instead of an artifact.

### Kinesis ESM attach-time binding (2026-10-27)

- **kinesis/esm-attach** — `CreateEventSourceMapping` for a `LATEST` kinesis
  source registers the mapping on the stream synchronously (mirroring
  moto's push-registration, `moto/kinesis/models.py`
  `KinesisStream.lambda_event_source_mappings`), freezing one per-shard
  `AT_SEQUENCE_NUMBER` watermark at attach time: a pre-attach record is no
  longer replayed into the mapping and no post-attach record is lost to a
  poller's first tick. The watermarks ride the mapping record itself
  (`EventSourceMapping.kinesis_bound_watermarks`), LATEST-only — `TRIM_HORIZON`
  keeps its replay contract — and a binding that would overshoot a shard's
  live next sequence number (stream re-created, shard counters reset) is
  discarded, rebinding at first poll sight (stale-rebind).

### CloudWatch alarm engine (2026-10-27)

- **cloudwatch/alarms** — metric alarms evaluate on the PutMetricData path:
  the trailing `EvaluationPeriods` windows of `Period` aggregate with the
  alarm's statistic, `DatapointsToAlarm`/`TreatMissingData` decide between
  ALARM / OK / INSUFFICIENT_DATA / hold-in-place per the documented
  real-CloudWatch rules, and every actual state transition delivers the
  alarm's `AlarmActions` / `OKActions` / `InsufficientDataActions` through
  SNS and Lambda with the real CloudWatch `AlarmName`/`NewStateValue`/
  `NewStateReason`/`OldStateValue` notification envelope. moto 5.2.4 keeps
  alarms static — this is the twin mirroring the real service.
- **compat** — datapoint candidates now respect the alarm's dimension set
  (a datapoint matches when its dimension set contains the alarm's — the
  same subset predicate `GetMetricStatistics` filters with): two alarms
  watching the same metric in one namespace no longer cross-fire on each
  other's dimension splits.

### Runtime families and images (2026-10-27)

- **lambda/runtime-families** — native in-process executors for the managed
  families: `ruby*` runs through `which ruby`, `java*` through javac+java
  (the bridge compiles once per process), `dotnet*` through the dotnet CLI;
  nodejs 18.x/20.x/22.x prefer their versioned `node18`/`node20`/`node22`
  binaries with a plain-`node` fallback (divergence logged). `GET
  /_robotocore/runtimes` reports what the running server can actually
  execute.
- **images/boto3-bundle** — the Docker images ship the boto3 site-packages
  inside the bundled lambda runtime directory, so a deployed function's
  `import boto3` runs from the image itself without vendored packages
  (158MB → 202MB default image; the `runtimes` stage carries it along with
  the versioned node binaries).

### CI tree-truthfulness (2026-10-27)

- **ci/tree-truth** — the CI, parity, IaC and app-integration lanes run the
  twin built from the code under test through the new
  `.github/actions/start-twin-from-source` composite (compile + health-wait
  on the checkout), never the published image — a regression cannot pass a
  gate on a stale artifact.
- **parity/probe-freshness** — `make probe-freshness` fails the lane when a
  `probes/<svc>.json` corpus is older than the newest commit touching that
  service's implementation; it runs as the first step of `make parity`, and
  a port lane is not done until its own scratch twin has been re-probed and
  the manifest/coverage artifacts regenerated.
- **release/runtimes-stage** — the release lane's `runtimes` image build
  targets the renamed `runtimes` Dockerfile stage, so `:runtimes` really
  carries the node + ruby executor toolchain it claims.

### Review hardening (2026-10-27)

- **review/zip-bomb** — a crafted 2MB deployment zip could expand past
  250MB into the extraction dir on every invoke and fill the disk: all
  executor families now enforce a running decompressed-bytes budget — the
  `zip` crate's `extract_zip_into` for the custom-runtime path, the same
  bound inside the Python shim and the Node bootstrap (checked pre-entry
  against the central-directory uncompressed sizes, aborting with a
  non-zero exit before the bomb writes) — and the stdin-fed family
  executors carry a dedicated 6MB result window so a large handler response
  no longer truncates into their 128KB log tail.
- **review/alarms** — hardening pass over the alarm engine: a re-put
  PutMetricAlarm resets the state the way moto's backend constructs a
  fresh Alarm (newly-on-action alarms evaluate from the OK base the reset
  makes observable), a staged action delivery re-validates the alarm's
  current state before each ARN (a Lambda action re-entering the twin
  mid-pass can no longer let the outer pass publish a stale notification),
  and query-protocol integer members parse as numeric strings — the
  query wire encodes Period/EvaluationPeriods scalars as strings, where a
  bare `as_i64` silently degraded them to the API defaults.
- **review/esm** — hardening pass over the ESM engine: the LATEST bind
  watermarks live on the mapping record (state save/restore carries them;
  no publish-then-bind window for a poll tick to race), only `LATEST`
  sources bind and `TRIM_HORIZON` mappings retain their replay-from-zero
  delivery, and a stream re-created under an existing mapping re-derives
  its watermarks instead of pinning to a dead shard counter.

## 2.0.0

The Python 2.x line is retired: robotocore 2.0 is a **pure-Rust twin**
(axum gateway + the `rotocore` crate) — the server ships no Python, no
moto sidecar, and no second process. Nothing is bridged: 160 of 160
registered services answer natively with moto-fidelity recorded per op
(9,929 implemented of 10,391 botocore ops; 406 denies — every deny is a
recorded decision in `tooling/surface-absence.json`).

### Bedrock AgentCore (issue #371, 2026-10-26)

- **port/iss-agentcore** — Amazon Bedrock AgentCore served natively for the
  first time: `bedrock-agentcore-control` (code interpreters, agent
  runtimes + versions + endpoints, gateways + targets, memories with
  strategy CRUD, tags) and `bedrock-agentcore` (memory events CRUD with
  moto's exact validation messages, code-interpreter session
  Start/Get/Stop/List, derived ListActors/ListSessions) registered as the
  159th and 160th native services. Both botocore models sign with scope
  `bedrock-agentcore`, so one rotocore backend splits the (disjoint) URI
  families; the introspection table carries both labels.
- **Exceeds moto**: moto 5.2.4 implements no code-interpreter surface at
  all (every `*CodeInterpreter*` op answers its 501); the twin serves the
  full control CRUD and the session family, including the lazily-seeded
  pre-provisioned SYSTEM interpreter `aws.codeinterpreter.v1` (undeletable).
  Runtime/gateway/memory CRUD mirrors moto's pinned backend shapes, ids and
  ManagedState transitions 1:1.
- **Genuinely absent**: `InvokeCodeInterpreter` (its return shape is an
  event stream — `CodeInterpreterStreamOutput`, unbidirectional-servable
  here exactly as in moto), the memory-record and extraction-job families,
  browser sessions, runtime invocations, workload tokens and the
  evaluators/policies/identity-provider surfaces — all recorded as
  modeled 501 denies in the probe corpora.

### Upstream-parity wave (2026-10-26)

- **port/up-ec2** — `RunInstances` launches `MaxCount` instances instead
  of the MinCount-only bias (moto-vs-AWS bug, `getmoto/moto#10215`).
- **port/up-events** — event patterns apply nested patterns per element
  of an object array, fields never mixing across elements (moto PR
  `getmoto/moto#10360`, event-ruler "JSON Array Matching").
- **port/up-dynamodb** — `CreateTable`/`DescribeTable` return the
  WarmThroughput block (moto `table.py` defaults; Terraform provider
  polling, localstack #13140).
- **port/up-sqs** — batch entry Id validation across the three batch ops
  (moto PR `getmoto/moto#10346`) and the 60-second `PurgeQueueInProgress`
  throttle with HTTP 403 (moto `getmoto/moto#10234`, 5.2.4).
- **port/up-s3-markers** — object keys percent-decode exactly once for
  storage, listing and tokens (localstack discussion 54, ex localstack
  #13745); GET/HEAD on delete markers carry the marker headers and a
  marker-version GET answers 405 MethodNotAllowed (moto PR
  `getmoto/moto#10428`).
- **port/up-dynamodb2** — WarmThroughput on every
  GlobalSecondaryIndexDescription and in the UpdateTable response (moto
  `dynamodb/models/table.py`; `update_table` → `table.describe()`).
- **port/up-swarm6** — s3control `CreateAccessPoint` Tags + token model
  (moto 5.2.4 changelog), kinesis `ListStreams` Limit/NextToken paging
  (moto PR `getmoto/moto#10385`), fsx `DeleteFileSystem` raises
  FileSystemNotFound (moto 5.2.4 changelog), vpclattice listener trio
  ported from the botocore model.
- **port/up-events2** — `create_partner_event_source()` returns the
  EventSourceArn (moto 5.2.2) and `list_event_buses()` gains
  Limit/NextToken pagination (moto 5.2.3, PAGINATION_MODEL).
- **port/up-tinies-b** — wafv2 `GetWebACL` accepts an ARN (moto 5.2.0),
  rds MonitoringRoleArn/MonitoringInterval combination validation
  (`getmoto/moto#10137` + `#10212`), glue `GetConnection` HidePassword +
  `GetConnections` Filter (moto 5.2.4).
- **port/up-tinies-a** — transfer describe responses omit unset members
  (moto 5.2.3 `getmoto/moto#10068`), route53 alias record sets carry no
  TTL (moto 5.2.2 `getmoto/moto#10029`), acm CN-less import tolerance +
  request_certificate Options merge (moto 5.2.2 `getmoto/moto#9912` +
  `getmoto/moto#10030`).
- **port/up-ecs** — `RunTask` honors capacityProviderStrategy with moto's
  launch-type combination rejection (moto 5.2.3) and DescribeServices
  returns rolloutState/rolloutStateReason/failedTasks (moto 5.2.0
  changelog, `MOTO_ECS_SERVICE_FAILED_TASKS`).
- **port/up-cognito** — `RespondToAuthChallenge` PASSWORD_VERIFIER bound
  to the initiate_auth session (moto PR `getmoto/moto#10243`, for
  `getmoto/moto#7562`).
- **port/up-kin-cw** — kinesis put_record/put_records publish
  IncomingBytes/IncomingRecords to CloudWatch in-process via the
  cloudwatch ingest bridge (moto 5.2.4 changelog,
  `KinesisBackend._send_stream_metrics`).
- **port/up-ddb3** — dynamodb enforces the 409,600-byte item-size limit
  with AWS Number byte sizing (`getmoto/moto#10176`), iot lists cap pages
  at AWS's 100-item maximum (moto PR `getmoto/moto#10417`), sqs batch
  validation precedes queue existence (moto's response-layer order).
- **port/up-s3-post** — the S3 POST-form (browser presigned-POST) wire
  path: multipart parsing, SigV4 policy-signature verification and policy
  condition enforcement with content-length-range and the covered-field
  rule (localstack `presigned_url.py::validate_post_policy`, AWS S3 POST
  docs; verification secret pinned per the divergence table below).
- **port/up-sfn** — native JSONata for `QueryLanguage: JSONATA` states
  (AWS Step Functions JSONata docs, JSONata 2.0.6 subset) and the
  AWS-dialect JSONPath filter operators (moto issue `getmoto/moto#10078`
  with `#7825` singleton shapes and localstack's singleton-collapse fix).
- **fix wave (port/fx-\* and port/fx2-\*)** — after each adversarial
  review round, verification lanes re-derived lane claims against the
  moto 5.2.4 oracle (`/home/termag/.moto-venv`, stock PyPI 5.2.4 — the
  same upstream line the deployed fork pinned) and live probe
  corpora: `port/fx-sqs`, `fx-ddb`, `fx-ecr`, `fx-ecscog`, `fx-events`,
  `fx-iotvp`, `fx-kinlim`, `fx-s3`, `fx-sfn`, `fx-tinies2`, the ECR
  layer-upload lane `fx-ecrlayer` (InitiateLayerUpload /
  UploadLayerPart / CompleteLayerUpload + stored layers behind
  BatchCheckLayerAvailability; `GetDownloadUrlForLayer` mirrors moto's
  `NotImplementedError` absence with the 501 genuine-gap envelope), and
  the round-2 lanes `fx2-*`; plus this documentation/comment truth lane.
- CloudTrail → EventBridge events for elbv2/autoscaling/ec2 mutations
  (#347).
- SSM SendCommand InstanceIds records per-instance invocations (#347).
- Cognito per-pool JWKS, RS256-signed tokens and OIDC discovery (#347).
- x-era Powertools redundant-parentheses fix (#283, verified at tip).

### Documented divergences in 2.0.0

- Paginators mint no `parameterChecksum` and validate none (see the
  last row below for what moto does).
- ACM: the first-SAN DomainName fallback for CN-less imports is not
  implemented (stays DomainName `""` / Subject `CN=`).
- S3 SigV2 POST signatures are accepted unverified (any secret passes).
- Cognito auth flows do not model the MFA challenge arms.
- Step Functions JSONata is a deliberate dialect subset (see
  `crates/stepfunctions/src/jsonata.rs`, "Deliberately not covered").
- S3 POST-upload SigV4 verification pins the signing secret to `test`
  (AGENTS.md "Endpoint & credentials" exception row).
- The 38-significant-digit cap is not enforced in DynamoDB Numbers
  (shared with moto).
- The fx-lane paginator token families (list_event_buses, kinesis
  ListStreams, s3control access points, the iot PAGINATION_MODEL lists,
  vpclattice list_listeners) mint no `parameterChecksum` and do not
  validate one — moto's paginator validates a checksum over the
  non-pagination kwargs and answers `Invalid Token: Input inconsistent
  with page token` even with `fail_on_invalid_token: False`
  (moto/utilities/paginator.py:115); moto's s3control paginators
  fail_closed (`fail_on_invalid_token: InvalidNextTokenException`, which
  the twin mirrors for undecodable tokens only). Unrelated to that
  family: the twin's `ds` paginator REQUIRES and validates moto's
  checksum, `mediapackagev2` mints one, and `cloudhsmv2` mints
  `parameterChecksum: 0` — mirroring each service's moto shape.

### Added

- **Full lambda language support.** python/nodejs/custom run in-process;
  every official AWS runtime image (python, nodejs, java, dotnet, go,
  ruby, provided) executes under `LAMBDA_EXECUTOR=docker`; EventBridge
  scheduled rules fire; ESM carries MessageAttributes + FilterCriteria
  operators; Task dispatch fans out to sqs/sns/dynamodb/states.
- **Two-image release model:** tiny distroless default (python-family
  runtimes in-image), Debian-slim `runtimes` (+node/ruby) and
  `java-and-dotnet` variants (OpenJDK 21 + .NET SDK 8).
- **Real security primitives:** KMS re-keyed to true AES-256-GCM
  ciphertext blobs with the moto wire layout and `EncryptionContext`
  bound as AAD; IAM policy evaluation (Effect/NotAction/conditions/
  variables/permissions-boundaries/cross-account combining).
- **Business-fidelity gates are machines, not docs:**
  `make parity` = probe-freshness → parity → coverage-gate (the error
  census is a live wire census run manually at wave boundaries), where
  probe corpora may not lag their implementations and every recorded deny
  must live-verify 501 `NotImplemented` (crash mirrors verified
  byte-exact against the moto fork).

### Changed

- The workflow modernization drops the python lanes; the
  `java-and-dotnet` image variant publishes alongside `:latest` and the
  version tag from the release lane (the standard image IS the `runtimes`
  stage).

### Fixed

See the 2026.10.x entries below (crash-fidelity canon, CFN mirror
rollback cascade, lambda docker executor hardening, iam/kms/sfn/events
semantic fixes from the adversarial review waves).

## 2026.10.24 (shipped in 2.0.0)

### Fixed: crashed probe rows now match moto's exact wire behavior

Triaged the committed probe artifacts' HTTP-500 rows against the moto
line that is robotocore's parity source (the JackDanger fork at
moto-5.1.23.dev then; today's oracle in `~/.moto-venv` is stock moto
5.2.4 from the same upstream line) op-by-op. Two moto-crash classes and one
twin-only divergence:

- **Ses un-routed operations were unreadable to XML SDKs.** The 16 SES
  operations the moto fork does not route (identity policies, tracking /
  delivery options, `SendBounce`, rule repositioning, ...) fell through to
  the gateway's JSON 501 document, which a query-protocol (XML) client
  cannot parse and therefore surfaced as a `500_error` crash row. The ses
  module now answers the twin-wide canonical 501 itself, in the
  query-protocol envelope: `NotImplemented: The <snake_op> action has not
  been implemented`, matching moto's `call_action` message.
- **Moto's own unhandled exceptions are now served moto-identically.**
  moto's unguarded `KeyError` / `AttributeError` / `ValueError` escapes
  leave the handler and tumble to the werkzeug server, which answers a
  plain HTML 500 page — no AWS envelope at all. Crash-faithful rows now
  render those exact bytes (`rotocore::proto::moto_crash_response`) for
  elb (`Register/DeregisterInstancesFromLoadBalancer`,
  `Attach/DetachLoadBalancerFromSubnets`,
  `Describe/ModifyLoadBalancerAttributes` when the load balancer is
  missing), athena (`GetNamedQuery`, `Get/StopQueryExecution`), dms
  (invalid filter names, `ListTagsForResource` selector misuse),
  emr (`SetTerminationProtection`, `SetVisibleToAllUsers`,
  `TerminateJobFlows`, unknown-instance-group paths), emrcontainers
  (malformed `containerProvider` bodies, bad next tokens), panorama
  (unknown devices), s3control (`PutBucketVersioning` — moto indexes
  `["PutBucketVersioningRequest"]` on a body whose root is always
  `VersioningConfiguration`).
- **Two elb crashes were twin-only and are now fixed** to moto's modeled
  behavior: `ApplySecurityGroupsToLoadBalancer` validates security groups
  through the EC2 loopback before the load-balancer lookup (moto raises
  `InvalidSecurityGroupError` → `ValidationError`; the loopback probe also
  used an EC2 filter encoding that never matched, so every id counted as
  existing), and `DescribeLoadBalancerPolicies` raises the modeled
  `LoadBalancerNotFound` like moto's `describe_load_balancers`.
- Unchanged-by-design rows: `resource-groups` tag-sync tasks (the twin
  already serves moto's byte-exact werkzeug page), `workspaces` unknown-id
  `InternalError` mirrors (recorded in probes and pinned by the compat
  suite), and the `connect` module's codified 501/`InternalError` surface
  for operations moto never routed.
- Probe artifacts (`probes/`) re-run through the probe runner for
  elb, ses, athena, dms, emr, emrcontainers, panorama, s3control and
  bedrock-runtime: ses drops from 16 crashed rows to
  16× `not_implemented` and elb from 8 to 7 (its kept crash rows now show
  moto's unparseable-HTML observables), while the 500 rows it keeps are
  byte-shape identical to what the moto server serves. `scripts/coverage.py`
  regenerated `tooling/coverage.json` and advanced the
  `tooling/coverage-budget.json` ratchet (`covered 9458/10268`, gate OK).

## 2026.10.19 (shipped in 2.0.0)

### Added: native EMR backend on the pure-Rust server

EMR moved from the moto sidecar to a native `rotocore` backend
(`rotocore/src/services/emr.rs`). The repo's botocore data pins EMR to the
JSON wire (`jsonVersion 1.1`, target-prefix `ElasticMapReduce`), so the
backend speaks JSON, not query-XML.

- 52 of the 60 spec operations are implemented: cluster lifecycle
  (RunJobFlow/Describe/List/Terminate/tags/visibility/concurrency), steps
  (Add/Describe/List/Cancel), instance groups and instance fleets, bootstrap
  actions, Auto-Scaling policies, managed-scaling and auto-termination
  policies, security configurations, block-public-access configuration,
  studios and studio session mappings, notebook-execution accessors,
  persistent app UIs, release labels, and per-release supported instance
  types (data embedded from moto's resource files).
- 8 operations deny with moto's exact `NotImplemented` document
  (`The <op> action has not been implemented`, HTTP 501):
  ListStudioSessionMappings, SetKeepJobFlowAliveWhenNoSteps,
  SetUnhealthyNodeReplacement, GetClusterSessionCredentials,
  GetPersistentAppUIPresignedURL, GetOnClusterAppUIPresignedURL,
  StartNotebookExecution, UpdateStudio.
- Error shapes mirror moto exactly, including its quirks: unknown clusters
  answer `InvalidRequestException`/`NoSuchCluster`, a few moto dict reads
  escape as KeyError → `InternalError` (500), keyword-form exceptions render
  the fallback message "An unspecified service error occurred", and the
  studio-session-mapping lookup keys interpolate an absent IdentityId as the
  string `"None"`.
- `test_emr_compat.py` holds 74 pass / 7 fail with the fail set identical to
  the moto-sidecar baseline; `tests/moto_impl/test_emr_lifecycle.py` passes
  11 / fails 5, matching the sidecar baseline exactly.

#### Migration

EMR calls to the Rust server no longer round-trip through the moto sidecar;
state is in-memory and per-(account, region) as before. `Instances.
InstanceFleets` on `RunJobFlow` stays a no-op (moto parity — fleets are
created with `AddInstanceFleet`).

## 2026.5.14 (shipped in 2.0.0)

### Added: state save/load/reset on the pure-Rust server

The Rust server now speaks the Python server's state-management surface:

- `GET|POST /_robotocore/state/save` — `{name, path?, services?}`; with a
  `path` writes `<path>/<name>.json` (unnamed: `<dir>/robotocore_state.json`),
  without one keeps an in-memory named snapshot; responses carry
  `{status: "saved", path, services}`.
- `POST /_robotocore/state/load` — `{name, path?, services?}`; restores from a
  saved file or an in-memory snapshot (`{status: "loaded"}`, or
  `{status: "no_state_found"}` when absent).
- `POST /_robotocore/state/reset` — clears every native service's state
  (`{status: "reset"}`).
- `GET /_robotocore/state/snapshots` (also `/robotocore/state/snapshots`)
  — lists in-memory named and versioned snapshots.
- `POST /_robotocore/state/snapshots/save|load` — versioned named snapshots
  (each save appends a revision; load restores a chosen or the latest one).
- `GET /_robotocore/state/export`, `POST /_robotocore/state/import` — JSON
  dump/reload channel (`native_state` envelope).

Every native service crate exposes a `snapshot_json` / `restore_json` /
`reset` surface (serde derives + lock-tolerant helpers keyed by
`account|region`), wired through a central registry built from the same
handler instances the wire path uses. File writes are atomic (temp +
rename). This is the last gateway feature the Python branch provided that
the Rust port lacked; the state/persistence/concurrency compat suites pass
at parity (20/20).

#### Migration

State endpoints previously returned `NotImplemented` on the Rust server;
they are now live. `POST /state/reset` wipes native service state for the
whole process (including cross-service handler instances), matching
`docker restart` semantics; callers that used the endpoint for
test-isolation must switch to unique account ids.

### Fixed: deployed-API invocation through the Rust gateway

Proxied (moto-sidecar) requests previously dropped the request path (they
arrived as `POST /`), which broke rest-json/rest-xml services such as API
Gateway resource/configuration calls with `501 NotImplemented`. The proxy
now forwards the original path + query and the `x-robotocore-service`
header. `/restapis/{api}/{stage}/_user_request_/{path}` invocations are
served by a rust-side executor that reads the deployed integration config
from the sidecar and dispatches `AWS_PROXY` integrations into the native
lambda crate, mirroring the Python executor.

## 2026.10.12 (shipped in 2.0.0) (python-era server)

IAM's policy simulator answered `allowed` for every action no matter what policies it was
given. `SimulateCustomPolicy` and `SimulatePrincipalPolicy` now evaluate those policies with
the IAM policy engine, so tests that ask the simulator "can this principal do X?" get the
answer AWS would give.

### Changed

- **`SimulateCustomPolicy` / `SimulatePrincipalPolicy` evaluate policies.** Results carry
  `allowed`, `explicitDeny` or `implicitDeny` (explicit Deny wins; a permissions boundary limits
  allows and is reported in `PermissionsBoundaryDecisionDetail`), `MatchedStatements` naming the
  source policy, `MissingContextValues` for condition keys a matching statement needed but
  `ContextEntries` did not supply, and one result per action and resource with
  `ResourceSpecificResults` when `ResourceArns` is given. `ResourcePolicy` is evaluated against
  `CallerArn`, a `ResourceOwner` in another account requires both sides to allow, and
  `MaxItems`/`Marker` paginate. `SimulatePrincipalPolicy` gathers a user's, group's or role's
  inline and attached managed policies (AWS-managed included), a user's group policies, and the
  attached permissions boundary; a missing principal returns `NoSuchEntity` (404) and a malformed
  input policy returns `InvalidInput` (400).

### Fixed

- **IAM policy wildcards and negated conditions.** `?` in an `Action` or `Resource` pattern
  matches exactly one character, as in AWS, instead of only a literal `?`. A negated condition
  operator (`StringNotEquals`, `StringNotLike`, `ArnNotLike`, `NotIpAddress`, ...) whose key is
  absent from the request is satisfied, per AWS's documented semantics; it previously never
  matched. This also applies to `ENFORCE_IAM=1` request enforcement.

#### Migration

Simulator calls that used to return `allowed` unconditionally now return `implicitDeny` or
`explicitDeny` when the policies do not grant the action. Note that with no `ResourceArns` the
simulated resource is `*`, which only statements with `"Resource": "*"` match. Under
`ENFORCE_IAM=1`, a Deny statement guarded by a negated operator now applies when the request
lacks the condition key.

## 2026.10.10 (shipped in 2.0.0) (python-era server)

### Added

- **SQS long polls stop starving other services.** `ReceiveMessage` waits run on a
  dedicated bounded pool (`SQS_LONGPOLL_THREADS`, default 16) instead of the shared
  default executor, so long polls can no longer queue every other service's work
  behind 20-second waits.
- **Auto-save never stalls the event loop.** Under `PERSISTENCE=1` the debounced
  snapshot runs on a worker thread (as `/state/save` already did); a request no
  longer wedges every other in-flight call behind a full state dump.
- **Unroutable requests get a `hints` list.** The 400 for requests whose AWS service cannot be
  resolved now names every routing cue the request carried — a bare unsigned `?Action=…` call
  learns that query-protocol services share that shape and that a SigV4 credential scope (or a
  service path) is what would route it; AGENTS.md documents it so agents do not count it as a
  service absence (only 501 `NotImplemented` is a gap).
- **Comprehensive health reporting.** `/_robotocore/health` and `/_localstack/health` report
  `running` or `disabled` per service (honouring the `SERVICES` env filter), so LocalStack-style
  tooling gating on health does not proceed into 501s.
- **`scripts/local-ci.sh`** — local replication of CI's lint/unit/integration/compat/cross-service/
  parity gates, with the live-server stages booting their own fresh robotocore on a private port.
- **`/_robotocore/config` reports `version`.** AGENTS.md and README direct agents at the
  endpoint for the emulator version; the field was previously absent from the payload.
- **`POST /_robotocore/ec2/capacity` validates its numeric fields and answers 400 with the
  field name**; `SQS_LONGPOLL_THREADS` tolerates a bad value (warn + default) so one wrong env
  var cannot brick server boot.

### Fixed

- **Cross-account in-memory stores were region- (or name-) keyed.** CloudWatch log metric
  and subscription filters, SESv2 email templates, X-Ray encryption config and resource
  policies, Kinesis `GetRecords`/shard iterators, Lambda event source mappings, Step
  Functions SQS/SNS integrations, EventBridge connections/API-destinations/endpoints,
  Firehose S3 delivery, and SQS delete/purge behavioral cooldowns all now key state by
  `(account_id, region)` (plus the entity name where multiple of one exist per queue). A
  second account in the same region no longer reads account A's filter destinations,
  template HTML, KMS key ids, or policy documents — and cannot overwrite or delete them.
- **CloudFormation XML escapes caller values.** Stack names, parameter/tag values and
  error messages were emitted into `DescribeStacks`/`ErrorResponse` verbatim, so an `&` in
  a parameter produced a document botocore could not parse. Rekognition face stores take
  a re-entrant lock so concurrent SearchFace/DeleteFace cannot mutate mid-iteration.
- **Moto snapshot restores keep Lambda functions.** moto's LambdaStorage holds functions
  in a `WeakValueDictionary` whose closures cannot pickle — the save loop skipped the
  whole `lambda` backend while `metadata.json` still claimed it. The restricted pickler now
  reduces WeakValueDictionaries to a live-entry snapshot and rebuilds them on load.
- **Tenant-level state reach**: the boot state component registers S3 (CORS, lifecycle,
  object-lock, legal-hold, logging, directory buckets) and Lambda (event source mappings,
  function URLs) save/load hooks; restarting with `ROBOTOCORE_STATE_DIR` previously reset
  those configs while queues/functions/buckets restored.
- **Considering AWS shape**: S3 `ObjectCreated` notifications carry the stored object's
  real size (moto's PUT response is empty so the header scrape read 0), the six sub-resource
  config handlers answer 404 `NoSuchBucket` like AWS (configs previously attached to
  nonexistent buckets leaked across accounts), the website 404 body escapes decoded keys
  and the `AssumeRole`/SAML paths register their minted ASIA keys so assumed-role calls
  resolve role policies under `ENFORCE_IAM=1` instead of implicit-denying.
- **Lambda account code-size accounting** releases on delete and replace-of-old, and
  `CodeStorageExceededException` answers 400 `CodeStorageExceededException` instead of
  500 `ServiceException`. Capacity chaos overrides honor their scope (instance type/AZ)
  at every consumption site, so a compat shard's synthetic override cannot inject
  `InsufficientInstanceCapacity` into an unrelated parallel worker's launches.

### Fixed

- **Provider crashes answer the AWS error contract.** A raise inside a native provider, the moto
  bridge, or a response handler used to escape every try/except and surface as Starlette's
  plain-text `Internal Server Error`; it now routes through the error normalizer.
- **500 responses use AWS's standard `InternalError`/`InternalFailure` codes** instead of leaking
  the Python exception class name; the specific exception stays in the `x-robotocore-diag`
  header — whose values are now ASCII-fenced (non-latin-1 exception text previously crashed the
  response encode and turned a 4xx into a 500).
- **JSON-protocol client errors stop producing 500s.** A body that json cannot decode answers
  400 `ValidationException`, and a JSON-protocol call missing `X-Amz-Target` answers
  400 `InvalidAction` — both previously crashed moto handlers (KeyError) and surfaced as 500.
- **Moto-bridge dispatch misses vs crashes are distinct.** A missing moto route keeps the 501
  `NotImplemented` contract ("only 501 is a gap"); other dispatch errors answer the 500
  contract with the diagnostic header, and moto's own error content type (XML with
  `X-Amzn-ErrorType`) is mirrored instead of declared JSON.
- **Unresolved-vs-failed health.** Services disabled by the `SERVICES` env filter report
  `disabled` in both health surfaces (was `running`/`available` regardless).
- **`@connections` API answers 405 with `Allow`** for methods other than GET/POST/DELETE (the
  handler previously fell through and crashed the caller with a None response).
- **Under `ENFORCE_IAM=1`, an unreachable IAM backend answers 500** with the diagnostic header
  instead of a 403 `AccessDenied` that blamed the caller for an emulator fault.
- **Unsigned form-encoded posts route to STS only for STS operations** (the one service AWS
  documents unsigned calls for); other unsigned form POSTs reach the unroutable 400 hints.
- **Chaos-injected errors and SQS error messages escape XML bodies** so `<` or `&` in a fault
  rule or exception text is readable output rather than a malformed document that the client
  cannot parse.
- **Validate fault rules on create**: an invalid operation regex in a chaos payload answers
  400 with the reason instead of crashing the admin route.

## 2026.10.8 (shipped in 2.0.0) (python-era server)

### Added

- **IPAM pool/scope filters honour `*` and `?` wildcards** in `DescribeIpamPools` and
  `DescribeIpamScopes` filter values, case-sensitive and any-of-the-list as AWS does, so Terraform
  data-source lookups that send `description=*…*` find their pool.
- **Tag filters on IPAM pools read the tag store correctly** — a tagged pool previously crashed
  the filter path (`TaggedEC2Resource.get_tags()` returns describe_tags dicts, not tag objects).

### Fixed

- **Routes report CoreNetworkArn.** `CreateRoute` and `ReplaceRoute` accept and echo
  `CoreNetworkArn` (Cloud WAN routing), so snapshot-driven IaC tools no longer see the
  route attribute as null after a write and re-propose in-place updates every plan.
- **VPC endpoint read-back reports ServiceRegion.** `CreateVpcEndpoint` accepts and echoes
  `ServiceRegion` (defaulting to the endpoint's own region), so snapshot-driven IaC tools no
  longer see the attribute as null after a write and force endpoint replacement on every plan.
- **VPC endpoint read-back reports IpAddressType and DnsOptions.** `CreateVpcEndpoint`
  and `ModifyVpcEndpoint` now accept and echo `IpAddressType` (default IPv4, response
  casing normalized) and `DnsOptions`/`DnsRecordIpType`, so snapshot-driven IaC tools no
  longer see the fields as unknown after every write and force endpoint replacement.
  An omitted `VpcEndpointType` also defaults to `Gateway` as AWS reports it; gateway
  endpoints correctly carry neither field. A security group deleted while still
  referenced by an endpoint no longer crashes the Describe read-back.
- **State snapshots load again with a Cognito user pool present.** The restricted
  unpickler rejected `joserfc` key classes (moto's cognito-idp stores the pool's signing
  key as a JWK), so every snapshot load failed with `_DisallowedClassError` — restore,
  pods and snapshot flows returned 500.
- **`ExecuteStatement` on a syntactically invalid PartiQL statement answers 400
  `ValidationException`** instead of crashing moto's parser (which surfaced as a 500
  InternalError). moto advanced to that commit.

## 2026.10.6 (shipped in 2.0.0) (python-era server)

### Added

- **Deterministic account ids for Organizations `CreateAccount`.** `POST
  /_robotocore/organizations/account-ids` (or `ROBOTOCORE_ORG_ACCOUNT_IDS=<json file>`) with
  `{"emails": {"<email>": "<12-digit id>"}, "names": {"<name>": "<id>"}}` makes `CreateAccount`
  assign that id instead of a random one, so an existing organization can be replayed into
  robotocore from its Terraform with every account keeping its id. `GET` returns the registered
  counts. Unregistered accounts still get random ids.

- **STS-issued credentials act in the role's account.** Calls signed with an `ASIA…` key from
  `AssumeRole` into `arn:aws:iam::<target>:role/X` (and IAM-user `AKIA…` keys) now resolve to the
  account that owns the key, as in AWS. Previously every such call fell through to the default
  account `123456789012`, so Terraform providers with `assume_role` plus `allowed_account_ids`
  failed with "AWS account ID not allowed" and resources landed in the wrong account.

#### Migration

Clients that assumed a role and then relied on resources appearing in `123456789012` will now find
them in the role's account instead.
- **State persistence keeps SSM and KMS.** The save loop skipped every service backend that
  failed to pickle, so SSM parameters (stored in moto `defaultdict`s with lambda or custom-init
  factories) and KMS keys (cryptography private-key objects) silently vanished across restarts.
  The pickler now rebuilds such maps with equivalent factories, preserves `defaultdict` subclasses
  like moto's `ParameterDict`, and round-trips private keys as PKCS#8 DER.

#### Migration

Snapshots taken before this release are missing SSM and KMS data; re-create those resources
after upgrading.
- **S3 bucket sub-resources accept a trailing slash.** aws-sdk-go-v2 (Terraform) sends path-style
  bucket requests as `/bucket/?cors=`; the native CORS, lifecycle, object-lock, legal-hold and
  logging handlers rejected that with `400 Bad Request`, so every `aws_s3_bucket` refresh failed
  on `GetBucketCors`. They now treat it as a bucket-level request (`404 NoSuchCORSConfiguration`
  when unconfigured).
- **S3 lifecycle configurations round-trip losslessly.** Rules are stored as written, so
  `NoncurrentVersionTransition`, `NewerNoncurrentVersions`, `Filter/And`, `ObjectSizeGreaterThan`
  and `ObjectSizeLessThan` come back from `GetBucketLifecycleConfiguration`. The
  `x-amz-transition-default-minimum-object-size` header is stored on Put and returned on Get/Put
  (default `all_storage_classes_128K`). Fixes Terraform's "Provider produced inconsistent result
  after apply" on `aws_s3_bucket_lifecycle_configuration`.
- **Organizations invitations work across accounts.** The invited account now sees the handshake
  (`ListHandshakesForAccount`, `DescribeHandshake`) and can `AcceptHandshake`/`DeclineHandshake`
  it; accepting an `INVITE` joins the account to the organization (`JoinedMethod: INVITED`), so
  the member's `DescribeOrganization` (Terraform's `data.aws_organizations_organization`) returns
  the organization instead of `AWSOrganizationsNotInUseException`. Organizations is now a native
  provider.
- **RAM shares work across accounts.** `GetResourceShares` and `ListResources` with
  `resourceOwner=OTHER-ACCOUNTS` (previously `501 NotImplemented`) return shares from other
  accounts whose principals cover the caller -- its account id or IAM ARN, its organization, or any
  OU on its path to the root (nested OUs included). Well-formed ARNs of every shareable type
  (including `ec2:ipam-pool` and ACM PCA `certificate-authority`) can be shared, and OU principals
  validate when the owner is a member account. RAM is now a native provider.
- **IPAM pools behave like an organization-wide IPAM.** `DescribeIpamPools` applies `Filter.N`
  (`description`, `locale`, `address-family`, `ipam-scope-id`, `owner-id`, `tag:*`, ...; Moto
  ignored filters) and includes pools shared with the caller through RAM, with `OwnerId`.
  `CreateVpc` with `Ipv4IpamPoolId` + `Ipv4NetmaskLength` allocates the first free block of the
  pool's provisioned space (own or shared) instead of failing with "Value (None) for parameter
  cidrBlock is invalid", and records the allocation. `DescribeIpamScopes` honours `IpamScopeId.N`
  and `Filter.N`, and returns `IpamScopeArn`.

- **SNS query-protocol responses are XML-escaped.** Attribute values, list members and error
  messages containing `&`, `<` or `>` (e.g. an HTTPS subscription endpoint with a query string)
  produced malformed XML that SDKs could not parse.
- **SNS `DisplayName` defaults to empty**, as in AWS, not to the topic name (Terraform showed a
  perpetual `display_name` diff on every `aws_sns_topic`).

#### Migration

`GetTopicAttributes` on a topic created without `DisplayName` now returns `""` instead of the
topic name.

- **CloudWatch over Smithy RPCv2 CBOR serves every operation.** aws-sdk-go-v2 (Terraform) speaks
  `rpc-v2-cbor` to CloudWatch; operations without a native handler (`PutMetricAlarm`,
  `DescribeAlarms`, `DeleteAlarms`, tagging, ...) returned `501 NotImplemented`. They are now
  bridged to Moto via AWS JSON 1.0 with model-driven timestamp/blob conversion; operations absent
  from the CloudWatch model return `UnknownOperationException`.

- **`GetFunctionCodeSigningConfig` on a function without one returns 200** with an empty
  `CodeSigningConfigArn`, as AWS does, instead of `404 ResourceNotFoundException`. Terraform reads
  it on every `aws_lambda_function` refresh, so every Lambda refresh failed.
- **Lambda layers resolve across accounts.** `GetLayerVersion` and `GetLayerVersionByArn` with
  another account's layer ARN resolve in the owning account (layer permissions are not modelled),
  so a function can reference a vendor's published layer.

#### Migration

Code that expected `GetFunctionCodeSigningConfig` to raise for an unconfigured function now
receives an empty ARN.

- **EventBridge keeps what you set.** `PutRule` stores `RoleArn` and creation-time `Tags`, and
  `DescribeRule` returns `RoleArn`/`CreatedBy`; `PutRule` on an existing rule updates it in place
  instead of dropping its targets. `CreateEventBus` stores `Description`, `KmsKeyIdentifier`,
  `DeadLetterConfig` and `Tags`, `DescribeEventBus` returns them, and `UpdateEventBus` is native.
  Fixes perpetual `role_arn`/`tags` diffs on `aws_cloudwatch_event_rule`/`aws_cloudwatch_event_bus`.

#### Migration

`PutRule` on an existing rule no longer removes the rule's targets, matching AWS.

- **SSM service settings are stored.** `UpdateServiceSetting` persists the value per account and
  region, `GetServiceSetting` returns it (`Status: Customized`) and accepts the setting's ARN as
  `SettingId` (previously an ARN nested in an ARN), and `ResetServiceSetting` restores the default.
- **`GetParameter` with another account's parameter ARN** resolves in the owning account (resource
  policies / RAM sharing are not modelled), so cross-account parameter reads work.

- **Hosted zones carry `Features.AcceleratedRecoveryStatus`** and `UpdateHostedZoneFeatures` is
  native. Terraform AWS provider >= 6.33 crashed reading a hosted zone without it.
- **`GetDNSSEC` returns the zone's key-signing keys and its signing status** (enable/disable is now
  tracked), and zone ids are normalized across KSK operations, so Terraform's
  `aws_route53_key_signing_key` waiter no longer times out.

- **`DescribeRegions` lists only the caller's partition** (`aws`, `aws-us-gov` or `aws-cn`), as AWS
  does. Moto listed GovCloud and China regions to commercial callers, so configurations that
  iterate over `data.aws_regions` built providers for regions outside their partition.
- **The VPC endpoint service catalog includes** `oidc-eks`, `sqs-fips`, `ec2-fips`,
  `acm-pca-fips`, `eks-fips` and `sts-fips`, so `data.aws_vpc_endpoint_service` lookups for them
  resolve.

- **Cognito `DeleteResourceServer`** is implemented (previously `501`), so Terraform can replace or
  destroy `aws_cognito_resource_server`.
- **EKS pod identity associations return `disableSessionTags` and a stable `externalId`**, so
  `aws_eks_pod_identity_association` no longer re-plans right after a clean apply.
- **`DescribeDBEngineVersions` serves a broader catalog** with prefix version matching (`16`
  matches `16.x`), `Filters` (e.g. `engine-mode`) and `DefaultOnly`, so `data.aws_rds_engine_version`
  lookups resolve.
- **EC2 fabrication admin plane (`POST /_robotocore/ec2/fabricate`).** Some real estates reference
  resources by a literal id the AWS wire protocol cannot mint (a peer VPC created outside Terraform,
  for example). The endpoint creates a real Moto-backed VPC with a caller-determined id in a chosen
  account and region, so `CreateVpcPeeringConnection` and every lookup that validates the referenced
  object behave as they would against the original environment. Idempotent: an already-existing id
  is returned in `skipped`, not duplicated.

- **AWS-managed IAM policies are attachable.** Naming `arn:aws:iam::aws:policy/...` in
  `AttachRolePolicy`/`AttachUserPolicy`/`AttachGroupPolicy`, `GetPolicy`, `GetPolicyVersion` or a
  permissions boundary now materializes that policy in the caller's account from moto's bundled
  catalog (plus a small supplement for newer policies), as it exists in every AWS account.
  `ListPolicies(Scope=Local)` is unchanged; `ListPolicies(Scope=AWS)` only lists policies that
  have been referenced.
- **`SetSecurityTokenServicePreferences` is reflected in `GetAccountSummary`**
  (`GlobalEndpointTokenVersion` 1 or 2), so `aws_iam_security_token_service_preferences` no longer
  shows a perpetual diff.

## 2026.8.26

### Changed

#### moto pinned to the fork's `robotocore/all-fixes` branch

`pyproject.toml` already resolved moto from `JackDanger/moto @ robotocore/all-fixes`,
but the `vendor/moto` submodule and `uv.lock` were still pinned to a commit on the
fork's `master` — a separate lineage that `all-fixes` does not contain. Since the
Docker build installs moto from the submodule (`moto = { path = "vendor/moto" }`),
the published image and the dev environment were resolving different moto trees.

All three now track `all-fixes` (moto `5.1.23.dev0` → `5.2.4.dev0`), and
`.gitmodules` records the branch so `git submodule update --remote` follows it.

##### Migration

`all-fixes` carries moto's migration from Jinja response templates to
modeled-shape serialization, so responses across many services are now closer to
the AWS wire format. Two behaviour changes are worth calling out:

- **Many previously-unimplemented operations now work.** Connect alone gained 117:
  operations that returned `501 NotImplemented` now either succeed or raise
  `ResourceNotFoundException` for unknown IDs, as AWS does. If your tests assert
  on `NotImplemented` for an operation, re-check it.
- **AWS Panorama is gone.** Upstream getmoto removed the deprecated service
  (getmoto/moto#10085). Panorama operations return `NotImplemented`.

### Removed

#### AWS Panorama

Deregistered. AWS discontinued the service and upstream getmoto removed its
implementation (getmoto/moto#10085), which the pinned `robotocore/all-fixes`
branch picks up. `panorama` is gone from the service registry, so its operations
now return `501`; its compat tests and probe data are deleted.

Service counts drop accordingly: **156 services** (46 native, 110 Moto-backed).
The counts in `README.md` and `CLAUDE.md` were already stale by one and are now
recomputed from the registry rather than hand-maintained.

### Fixed

- **`uv.lock` was unparseable.** An earlier merge left a duplicated `stevedore`
  package block, and `uv` refused to read the file at all (`Dependency 'stevedore'
  has missing 'source' field but has more than one matching package`). Regenerated.
- **Lambda .NET on slim images.** `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT` was set
  at the image level but three code paths in the .NET runtime built their own
  environment without it — the `dotnet --list-runtimes` probe passed no `env=` at
  all. dotnet then needed libicu, whose package name moves between Debian releases,
  and crashed. All dotnet subprocesses now go through one `_dotnet_compile_env()`.
- Upstream fixes to moto for CloudDirectory (missing root object broke every
  `ObjectReference` operation), CloudFront (ten handlers calling a removed
  `response_template()`, plus config accessors the serializer needs), Connect
  (two handlers reading parameters absent from their input shapes), EC2 (five
  handlers reading dict-returning backends as objects) and S3 (the lost Metadata
  Tables stubs, which made `CreateBucketMetadataConfiguration` fall through to the
  browser-upload path).
- **S3 object retention.** `GetObjectRetention` was missing from the object `GET`
  dispatch, so it fell through to `GetObject` and returned the object's bytes —
  callers saw a parse failure surfaced as a 500. Its response also placed
  `Mode`/`RetainUntilDate` at the top level instead of inside `Retention`.
- **S3Control batch jobs.** `UpdateJobPriority` and `UpdateJobStatus` read their
  inputs from the request body, but the model locates both in the querystring, so
  every call applied priority `0` and an empty status.
- **Two endpoints shared by two operations** were always dispatched to the sibling,
  because the query marker that separates them is invisible to the URI matcher:
  `POST /apikeys?mode=import` (ImportApiKeys) and
  `POST /backup-vaults/{name}/mpaApprovalTeam?delete`
  (DisassociateBackupVaultMpaApprovalTeam).
- **Lost state and operations** restored in moto: CloudDirectory's root object,
  NetworkManager's per-network collections (every `CreateConnection` and
  `Associate*` call 500'd), ServiceCatalog's initial provisioning artifact (so
  products had none), `KeyGroup.update()`, LakeFormation's `UpdateDataCellsFilter`
  and `StartQueryPlanning`, and S3's Metadata Tables stubs.
- **Unknown identifiers now raise the modelled error** rather than a 500, across
  RDS (5 lookups), EC2's `ModifyTransitGatewayVpcAttachment`, AutoScaling's
  `DisableMetricsCollection`, KinesisAnalyticsV2's `DescribeApplication` and
  MediaStore's `ListTagsForResource` (which also now resolves an ARN, not just a
  name).

## 2026.8.24

### Added

#### EC2 Capacity Profiles - Deterministic capacity management

Configurable EC2 capacity model for testing launch workflows, spot fallback, and `InsufficientInstanceCapacity` handling.

- New admin endpoints under `/_robotocore/ec2/capacity`:
  - `GET /_robotocore/ec2/capacity` — List capacity profiles
  - `POST /_robotocore/ec2/capacity` — Set capacity profile
  - `DELETE /_robotocore/ec2/capacity` — Delete capacity profile
  - `POST /_robotocore/ec2/capacity/reset` — Reset all capacity profiles
  - `POST /_robotocore/ec2/capacity/chaos` — Set chaos override for capacity
- Capacity model per (instance-type, availability-zone):
  - `total_capacity` — Maximum instances that can be launched
  - `available_capacity` — Current available capacity
  - `spot_available` — Whether spot instances are available
  - `spot_price` — Spot price when available
  - `enabled` — Whether the offering exists (for unsupported errors)
- `RunInstances` now checks capacity and returns proper AWS error codes:
  - `InsufficientInstanceCapacity` (HTTP 500) when capacity exhausted
  - `Unsupported` (HTTP 400) for disabled offerings
- `RequestSpotInstances` with deterministic spot fulfillment:
  - Returns `capacity-not-available` status when spot unavailable
  - Returns `fulfilled` status with instance ID when spot available
- Chaos integration: Capacity rules can be overridden via chaos endpoint
- State snapshot integration: Capacity profiles round-trip with save/load

- **EC2 Fast Snapshot Restore (FSR) support**: Implemented `EnableFastSnapshotRestores`,
  `DisableFastSnapshotRestores`, and `DescribeFastSnapshotRestores` operations with
  proper state machine modeling (`enabling` → `optimizing` → `enabled`; `disabling` → `disabled`).
  FSR state is tracked per (snapshot-id, availability-zone) pair.
- **VolumeInitializationRate support**: `CreateVolume` now accepts `VolumeInitializationRate`
  parameter when creating volumes from snapshots, with validation (100-300 range) and
  proper error responses.
- **Volume hydration state modeling**: Volumes created from snapshots now track a
  deterministic hydration profile — `cold` (lazy-loaded), `initialized` (fully hydrated),
  or `fsr-backed` (instant-ready via FSR). This state is exposed via the provider's
  `get_volume_hydration_state()` function for testing/inspection.
- **Chaos and audit integration**: FSR state transitions and CreateVolume operations
  are wired through the existing chaos-engineering (`POST /_robotocore/chaos/rules`)
  and audit-log (`GET /_robotocore/audit`) interfaces.

- **ECR OCI Registry v2 data plane** — Docker/ECR-compatible registry endpoints for pushing and pulling container images:
  - `GET /v2/` — Registry availability check
  - `GET/PUT/DELETE /v2/{name}/manifests/{reference}` — Manifest operations by tag or digest
  - `GET/HEAD /v2/{name}/blobs/{digest}` — Blob (layer) operations
  - `POST/PATCH/PUT/DELETE /v2/{name}/blobs/uploads/{uuid}` — Blob upload session flow
  - `GET /v2/{name}/tags/list` — List all tags in a repository
  - Bearer token and Basic auth support compatible with ECR's `GetAuthorizationToken` credential
  - Image tag immutability enforcement (`imageTagMutability=IMMUTABLE`)
  - Full integration with existing ECR control plane — images pushed via `/v2/` are visible to `BatchGetImage`, `ListImages`, etc.

#### EC2 Guest Executor - Pluggable user-data execution

New opt-in feature that executes EC2 instance user-data (cloud-init scripts) inside real containers. When enabled via `ROBOTOCORE_EC2_GUEST_EXECUTOR=1`, `RunInstances` launches a guest container for each instance and executes the user-data payload, capturing stdout/stderr/exit-code for each command.

**Features:**
- **User-data parsing**: Supports plain shell scripts (`#!`), MIME multi-part messages (cloud-init format), and base64-encoded data
- **Container execution**: Spins up containers using a configurable image (default: `jrei/systemd-ubuntu:22.04`) to execute user-data
- **Block device support**: Instance `BlockDeviceMappings` are exposed as volumes inside the guest container
- **IMDS support**: Instance metadata service endpoints available inside the guest (instance-id, instance-type, IAM credentials)
- **Systemd compatibility**: Guest containers run systemd for service management (`systemctl start/enable`)
- **Execution evidence**: Structured capture of stdout/stderr/exit-code per command, retrievable via `/_robotocore/ec2/guest/executions/{instance_id}`

**Configuration:**
- `ROBOTOCORE_EC2_GUEST_EXECUTOR=1` - Enable guest execution (default: disabled)
- `ROBOTOCORE_EC2_GUEST_IMAGE` - Container image for guests (default: `jrei/systemd-ubuntu:22.04`)

**API Endpoints:**
- `GET /_robotocore/ec2/guest/executions` - List all executions
- `GET /_robotocore/ec2/guest/executions/{instance_id}` - Get execution details for an instance

**Design:**
- Zero overhead when disabled - default EC2 behavior unchanged
- Guest execution is opt-in per environment
- Containers are torn down when instances are terminated
- Thread-safe execution tracking

#### Packer-compatible virtual instance transport

New opt-in container-backed EC2 instance transport for Packer's `amazon-ebs` builder.

- SSH and SSM transport support for provisioner connectivity
- File upload with proper destination path semantics (file vs directory validation)
- Shell provisioner execution with environment variables and working directory
- AMI creation with identity clearing (machine-id, hostname, SSH host keys)
- Filesystem state persistence to `/opt/ami-state` for AMI contents
- Fresh identity for instances launched from AMIs
- Enable with `ROBOTOCORE_PACKER_TRANSPORT=1` environment variable

## 2026.5.13 (2026-05-13)

### Breaking: Docker Hub image renamed to `jackdanger/robotocore`

The `robotocore/robotocore` organisation on Docker Hub costs $15/month
for a new org; we've moved Docker Hub publishing to the personal
namespace `jackdanger/robotocore` instead. **All Docker Hub image
references must update.** The GHCR image at
`ghcr.io/robotocore/robotocore:*` is **unchanged** and remains the
canonical pull location for users who prefer GitHub Container Registry.

```diff
- docker run -p 4566:4566 robotocore/robotocore:latest
+ docker run -p 4566:4566 jackdanger/robotocore:latest

# GHCR alternative (unchanged):
  docker run -p 4566:4566 ghcr.io/robotocore/robotocore:latest
```

Migrated references include the release workflow, all README/AGENTS/
LOCALSTACK docs, every per-service README, the docker extension,
``robotocore.cli.DEFAULT_IMAGE``, and the in-test skill docs.

### Major: real per-version dispatch for every Lambda runtime

Robotocore previously ran every Lambda function on whatever single
interpreter was baked into the image — a `python3.10` Lambda would
silently execute under the host's Python 3.12. This release ships
**faithful per-version execution** for all five multi-version Lambda
runtime families: every Lambda runtime identifier AWS supports now
either runs on the matching binary in the image, or installs the
matching binary on first invocation.

Supported Lambda runtime IDs and dispatch source:

| Family   | Runtimes | Default (baked) | Fault-in source                    |
| -------- | -------- | --------------- | ---------------------------------- |
| Node.js  | `nodejs18.x`, `nodejs20.x`, `nodejs22.x` | Node 20 | nodejs.org official tarballs |
| Python   | `python3.8`–`python3.13` | host Python 3.12 (in-process) | astral-sh/python-build-standalone |
| Java     | `java8`, `java8.al2`, `java11`, `java17`, `java21` | Temurin JDK 21 | Adoptium API |
| .NET     | `dotnet6`, `dotnet8`, `dotnet9` | SDK 9.0 | Microsoft's `dotnet-install.sh` |
| Ruby     | `ruby3.2`, `ruby3.3`, `ruby3.4` | Ruby 3.4 | Docker Registry pull from `ruby:X-slim` |
| Custom   | `provided.al2`, `provided.al2023` | n/a (user's bootstrap) | n/a |

#### Added

- **Per-runtime executor caching** — `get_executor_for_runtime("ruby3.3")`
  and `get_executor_for_runtime("ruby3.4")` now return distinct cached
  instances threaded with the requested runtime ID. Same for Node.js,
  Java, .NET, Python. (`custom` still shares one instance — there's no
  per-version concept for `provided.*`.)
- **Versioned binary resolution** in `_resolve_binary()` across
  Node/Ruby/Java with per-family `_RUNTIME_BINARY` maps. The .NET
  executor uses `_detect_tfm(runtime)` to pick the matching target
  framework moniker; Python's `PythonExecutor` adds a subprocess
  dispatch path for runtimes that differ from the host Python.
- **Fault-in install framework** (`src/robotocore/services/lambda_/runtimes/install.py`):
  - `InstallPlan` dataclass + per-language plan modules
    (`install_{java,node,python,dotnet,ruby}.py`).
  - `ensure_installed(runtime)` — idempotent, `flock`-protected against
    concurrent installs, blocks the triggering invocation, logs
    progress.
  - Stdlib-only Docker Registry HTTP client for Ruby's source layer pull
    (no `docker` daemon or `skopeo` dependency).
- **New endpoints**:
  - `GET /_robotocore/runtimes` adds per-runtime `status`
    (`installed` | `available_to_install` | `unavailable`) and a
    `faultin_disabled` flag.
  - `POST /_robotocore/runtimes/install` `{"runtimes": [...]}` — pre-warm
    one or more runtimes synchronously. Useful in CI setup so the first
    real Lambda invocation doesn't pay the download cost.
- **Config env vars**:
  - `ROBOTOCORE_RUNTIME_CACHE_DIR` (default: `/var/lib/robotocore/runtimes`)
  - `ROBOTOCORE_RUNTIME_BIN_DIR` (default: `/var/lib/robotocore/bin`,
    prepended to `$PATH` in the image)
  - `ROBOTOCORE_RUNTIME_DOWNLOAD_TIMEOUT` seconds (default: 300)
  - `ROBOTOCORE_RUNTIME_FAULTIN=disabled` to opt out (air-gapped CI).
- **Honest reporting**: `versions[family]` in `/_robotocore/runtimes`
  means "robotocore can faithfully execute this Lambda runtime" — not
  "this binary is installed somewhere". Faulted-in runtimes appear
  after install completes.
- **Divergence warnings**: when a requested runtime resolves to a
  different binary (default, or fault-in install failed), the executor
  logs a warning naming both the requested and actual runtime so
  version mismatch is never silent.

#### Changed

- **Docker images are smaller than pre-feature `main`** despite
  shipping true per-version dispatch:
  - `robotocore:latest` (standard): **722 MB → 463 MB**
  - `robotocore:java-and-dotnet`: **1,578 MB → 1,320 MB**
- The standard image is now well under the CI 500 MB target line that
  had been warning for months.
- `_detect_tfm()` in the .NET executor now picks the TFM matching the
  requested runtime when its SDK is installed, falling back to host max
  (with a warning) when missing. Module-level caches are invalidated
  after fault-in installs so newly-installed SDKs are immediately
  visible.
- The runtimes endpoint no longer advertises versions whose execution
  would silently downgrade — what's reported as `installed` is exactly
  what can be executed faithfully.

#### Fixed

- `Bootstrap.java` is now compiled with `--release 8` so the cached
  `Bootstrap.class` loads on any JVM major from 8 onward — fixes
  `ClassFormatError` that would have broken faulted-in `java8`/`java11`/`java17`
  JREs against the bytecode produced by the baked JDK 21 compiler.
- Unified `DOTNET_ROOT` so the baked SDK 9.0 and faulted-in
  6.0/8.0 SDKs all live under one dotnet host root — fixes the
  cross-root invisibility that would have made faulted-in SDKs
  unusable by the existing `/usr/local/bin/dotnet`.
- Fault-in tar extraction preserves the execute bit on binaries
  (was using `set_attrs=False` which stripped it — first invocation
  of any faulted-in runtime would have exited with `Permission denied`).

#### Migration

No breaking changes. Existing Lambda functions that worked before keep
working: the executor falls back to the host's default binary with a
warning if the requested runtime can't be installed. Behaviour change
to watch:

- Functions that *relied* on the previous silent version mismatch
  (e.g. a `python3.10` function that quietly ran on 3.12) will now
  install Python 3.10 on first invocation and execute under it. Any
  3.10-vs-3.12 stdlib or syntax differences will surface.
- Air-gapped environments: set `ROBOTOCORE_RUNTIME_FAULTIN=disabled`
  and pre-warm needed runtimes via `POST /_robotocore/runtimes/install`
  during image build (or stay on baked defaults).

## 1.0.0 (2026-03-07)

### Overview

First GA release. Robotocore is an MIT-licensed, open-source AWS emulator built on Moto with drop-in LocalStack compatibility. Single Docker container, runs on ARM Mac, no registration, no telemetry.

### Service Coverage

- **147 registered services** (38 native + 109 Moto-backed)
- **158 services with automated compat tests** (100% of the fleet-gated surface)
- **94/94 smoke tests passing**
- **5195+ total tests** (2520 unit + 2675 compat + 42 integration), 0 failures

### Native Providers (38)

Full behavioral fidelity with real execution semantics:

acm, apigateway, apigatewayv2, appsync, batch, cloudformation, cloudwatch,
cognito-idp, config, dynamodb, dynamodbstreams, ec2, ecr, ecs, es, events,
firehose, iam, kinesis, lambda, logs, opensearch, rekognition, resource-groups,
resourcegroupstaggingapi, route53, s3, scheduler, secretsmanager, ses, sesv2,
sns, sqs, ssm, stepfunctions, sts, support, xray

### Moto-backed Services (109)

Routing and request handling via Moto backends with protocol translation:

account, acmpca, amp, apigatewaymanagementapi, applicationautoscaling,
appmesh, athena, autoscaling, backup, bedrock, bedrockagent, budgets, ce,
clouddirectory, cloudfront, cloudhsmv2, cloudtrail, codebuild, codecommit,
codedeploy, codepipeline, cognitoidentity, comprehend, connect,
connectcampaigns, databrew, datapipeline, datasync, dax, dms, ds, dsql,
ec2instanceconnect, efs, eks, elasticache, elasticbeanstalk, elb, elbv2, emr,
emrcontainers, emrserverless, fsx, glacier, glue, greengrass, guardduty,
identitystore, inspector2, iot, iotdata, ivs, kafka, kinesisanalyticsv2,
kinesisvideo, kms, lakeformation, lexv2models, macie2, managedblockchain,
mediaconnect, medialive, mediapackage, mediapackagev2, mediastore, memorydb,
mq, networkfirewall, networkmanager, opensearchserverless, organizations, osis,
panorama, pinpoint, pipes, polly, quicksight, ram, rds, rdsdata, redshift,
redshiftdata, resiliencehub, route53domains, route53resolver, s3control,
s3tables, s3vectors, sagemaker, securityhub, servicecatalog,
servicecatalogappregistry, servicediscovery, ses, shield, signer, ssoadmin,
swf, synthetics, textract, timestreaminfluxdb, timestreamquery, timestreamwrite,
transcribe, transfer, vpclattice, wafv2, workspaces, workspacesweb

### Infrastructure Features

- **Gateway**: Single port (4566), full AWS protocol support (query, json, rest-json, rest-xml, ec2)
- **Chaos Engineering**: Inject ThrottlingException, latency, and custom faults via `/_robotocore/chaos/rules`
- **Resource Browser**: Cross-service resource overview via `/_robotocore/resources`
- **Audit Log**: Ring buffer of recent API calls via `/_robotocore/audit`
- **State Snapshots**: Named save/load with selective persistence via `/_robotocore/state/*`
- **IAM Enforcement**: Opt-in full policy evaluation engine via `ENFORCE_IAM=1`
- **Extensions**: Plugin system with entry point, env var, and directory discovery
- **Observability**: Structured JSON logging, tracing middleware, metrics, init hooks
- **Docker**: Multi-arch image (amd64/arm64), boots in <5s

### Migrating from LocalStack

```bash
# Before (LocalStack)
docker run -p 4566:4566 localstack/localstack

# After (Robotocore)
docker run -p 4566:4566 robotocore

# No code changes needed — same port, same endpoint URL, same AWS CLI flags
aws --endpoint-url=http://localhost:4566 s3 ls
```

### Known Limitations

The following services are NOT registered (Moto backends exist but all operations fail):
directconnect, ebs, forecast, personalize, sdb, servicequotas,
meteringmarketplace, sagemakermetrics, sagemakerruntime,
kinesisvideoarchivedmedia, mediastoredata

These may be added in future releases as Moto adds support.
