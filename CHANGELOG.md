# Changelog

This file follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Robotocore releases use CalVer (`YYYY.M.D[.N]`) — every push to `main`
auto-tags and publishes a versioned + `:latest` Docker image. Each release
gets a top-level section here; the project source of truth for the
maintenance policy is [`CLAUDE.md`](CLAUDE.md) under *Changelog discipline*.

## 2026.5.13 (2026-05-13)

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
