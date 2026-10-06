# Changelog

This file follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Robotocore releases use CalVer (`YYYY.M.D[.N]`) — every push to `main`
auto-tags and publishes a versioned + `:latest` Docker image. Each release
gets a top-level section here; the project source of truth for the
maintenance policy is [`CLAUDE.md`](CLAUDE.md) under *Changelog discipline*.

## 2026.10.6

### Added

- **Deterministic account ids for Organizations `CreateAccount`.** `POST
  /_robotocore/organizations/account-ids` (or `ROBOTOCORE_ORG_ACCOUNT_IDS=<json file>`) with
  `{"emails": {"<email>": "<12-digit id>"}, "names": {"<name>": "<id>"}}` makes `CreateAccount`
  assign that id instead of a random one, so an existing organization can be replayed into
  robotocore from its Terraform with every account keeping its id. `GET` returns the registered
  counts. Unregistered accounts still get random ids.

### Fixed

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
- **147 services with automated compat tests** (100% coverage)
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
