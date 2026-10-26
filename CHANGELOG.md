# Changelog

This file follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Robotocore releases historically use CalVer (`YYYY.M.D[.N]`) and now carry
semver majors; pushes to `main` auto-tag and publish versioned + `:latest`
Docker images. Each release gets a top-level section here; the project
source of truth for the maintenance policy is [`CLAUDE.md`](CLAUDE.md)
under *Changelog discipline*.

## 2.0.0

The Python 2.x line is retired: robotocore 2.0 is a **pure-Rust twin**
(axum gateway + the `rotocore` crate) — the server ships no Python, no
moto sidecar, and no second process. Nothing is bridged: 158 of 158
registered services answer natively with moto-fidelity recorded per op
(9,676 working of 10,268 botocore ops; every deny is a recorded decision
in `tooling/surface-absence.json`).

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
  `make parity` = probe-freshness → parity → coverage-gate →
  (nightly) error-census, where probe corpora may not lag their
  implementations and every recorded deny must live-verify 501
  `NotImplemented` (crash mirrors verified byte-exact against the moto
  fork).

### Changed

- The workflow modernization drops the python lanes; the
  `java-and-dotnet` image variant publishes alongside `:latest` and
  `:runtimes` from the release lane.

### Fixed

See the 2026.10.x entries below (crash-fidelity canon, CFN mirror
rollback cascade, lambda docker executor hardening, iam/kms/sfn/events
semantic fixes from the adversarial review waves).

## 2026.10.24 (unreleased)

### Fixed: crashed probe rows now match moto's exact wire behavior

Triaged the committed probe artifacts' HTTP-500 rows against the moto fork
(5.1.23.dev, the parity source) op-by-op. Two moto-crash classes and one
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

## 2026.10.19 (unreleased, `port/rot-emr2` branch)

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

## 2026.5.14 (unreleased, `port/rot-state` branch)

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
