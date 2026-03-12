# Migrating from LocalStack to Robotocore

## Context

LocalStack has been the standard open-source AWS emulator since 2017. Its Community Edition provided a free, no-strings-attached Docker image that thousands of teams relied on for local development and CI.

In early 2026, LocalStack [announced](https://blog.localstack.cloud/the-road-ahead-for-localstack/) that Community Edition support would end on March 23, 2026. The project is consolidating to a single Docker image that requires an authentication token. A free tier continues for non-commercial use, students (verified via GitHub), and open-source projects, but the unrestricted Community image will no longer receive updates.

This is a reasonable business decision. LocalStack has invested years building a sophisticated product and employs a team to maintain it. The Pro/Team/Enterprise tiers offer real value — Cloud Pods, IAM enforcement, Lambda layers, 100+ services, ephemeral environments, and more.

Robotocore exists for teams that want a **permanently free, MIT-licensed** AWS emulator with no registration requirement. It is not a fork of LocalStack — it is a new project built on [Moto](https://github.com/getmoto/moto) by a Moto maintainer.

## What you keep

If you're on LocalStack Community today, here's what carries over directly:

- **Same port**: 4566 — no client changes needed
- **Same SDK configuration**: `endpoint_url="http://localhost:4566"` works unchanged
- **Same credential handling**: dummy credentials, `AWS_ACCESS_KEY_ID=test`
- **Same Docker workflow**: `docker run -p 4566:4566 <image>`
- **All ~25 Community services**: every service in Community Edition is available in Robotocore

## What you gain

| Feature | LocalStack Community | Robotocore |
|---------|---------------------|------------|
| Services | ~25 | 147 (38 native + 109 Moto-backed) |
| Lambda execution | Yes | Yes (in-process Python) |
| SQS behavioral fidelity | Basic | Real visibility timeouts, PurgeInProgress, DLQ |
| IAM enforcement | No (Pro) | Yes (opt-in: `ENFORCE_IAM=1`) |
| State snapshots | No (Pro: Cloud Pods) | Yes (`/_robotocore/state/save` and `/load`) |
| Chaos engineering | No (Pro) | Yes (`/_robotocore/chaos/rules`) |
| Audit log | No | Yes (`/_robotocore/audit`) |
| Resource browser | No (Pro) | Yes (`/_robotocore/resources`) |
| Auth token | Required after March 2026 | Never |
| License | Apache 2.0 (sunsetting) | MIT |

## What you lose

Be honest with yourself about what LocalStack does well that Robotocore doesn't match yet:

- **`localstack` CLI**: no equivalent. Use `docker run/stop` and `aws --endpoint-url` directly.
- **`awslocal` wrapper**: use `export AWS_ENDPOINT_URL=http://localhost:4566` instead (works with any SDK).
- **Cloud Pods** (Pro): Robotocore has basic state save/load, but not versioned, remotely-shareable snapshots with team collaboration features.
- **Lambda Docker execution**: LocalStack can run Lambda functions in isolated Docker containers with full runtime parity. Robotocore executes Python Lambdas in-process, which is faster but doesn't support non-Python runtimes with full fidelity.
- **Mature ecosystem**: LocalStack has Terraform provider integrations, CI/CD plugins, Testcontainers modules, and a large community. Robotocore is new.
- **Web dashboard**: LocalStack Pro has a cloud-hosted dashboard for inspecting resources. Robotocore has a JSON resource browser API but no GUI.
- **HTTPS/TLS**: LocalStack supports TLS termination. Robotocore serves HTTP only.
- **`SERVICES` env var**: LocalStack lets you selectively enable services. Robotocore runs all 147 services always (unused services consume negligible resources, but there's no way to disable them).

## Quick start

```bash
# Before
docker run -p 4566:4566 localstack/localstack

# After
docker run -p 4566:4566 robotocore/robotocore
```

That's it. Same port, same endpoint, same credentials.

## Configuration mapping

| LocalStack | Robotocore | Notes |
|---|---|---|
| `GATEWAY_LISTEN=:4566` | Default | Same port |
| `SERVICES=s3,sqs` | All always on | No subsetting |
| `DEBUG=1` | `ROBOTOCORE_LOG_LEVEL=DEBUG` | |
| `PERSISTENCE=1` | Snapshot API | `POST /_robotocore/state/save` |
| `LAMBDA_EXECUTOR=local` | Default | In-process execution |
| `DEFAULT_REGION` | `AWS_DEFAULT_REGION` | Standard AWS env var |
| `ENFORCE_IAM=1` | `ENFORCE_IAM=1` | Same |
| `LOCALSTACK_API_KEY` | Not needed | No auth, ever |
| `LOCALSTACK_HOST` | `AWS_ENDPOINT_URL` | Standard AWS env var |

## Docker Compose

```yaml
services:
  # Before
  # localstack:
  #   image: localstack/localstack
  #   ports:
  #     - "4566:4566"

  # After
  aws:
    image: robotocore/robotocore
    ports:
      - "4566:4566"

  app:
    build: .
    environment:
      - AWS_ENDPOINT_URL=http://aws:4566
      - AWS_ACCESS_KEY_ID=123456789012
      - AWS_SECRET_ACCESS_KEY=test
      - AWS_DEFAULT_REGION=us-east-1
```

## Verifying the migration

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

aws sts get-caller-identity
aws s3 mb s3://test-bucket && aws s3 ls
aws sqs create-queue --queue-name test && aws sqs list-queues
aws dynamodb list-tables
curl -s http://localhost:4566/_robotocore/health | python3 -m json.tool
```

## Internal API differences

| LocalStack | Robotocore |
|---|---|
| `/_localstack/health` | `/_robotocore/health` |
| `/_localstack/diagnose` | `/_robotocore/config` |
| N/A | `/_robotocore/audit` |
| N/A | `/_robotocore/resources` |
| N/A | `/_robotocore/chaos/rules` |
| N/A | `/_robotocore/state/save` |
| N/A | `/_robotocore/state/load` |
| N/A | `/_robotocore/state/reset` |

## When to stay on LocalStack

If any of these apply, LocalStack Pro may be the better choice for your team:

- You need **non-Python Lambda runtimes** (Node.js, Java, Go, .NET) with full container isolation
- You depend on **Cloud Pods** for team-shared state snapshots with versioning
- You use **LocalStack's Terraform provider** or **cdklocal** extensively and switching tools has high cost
- You need the **web dashboard** for non-engineer team members to inspect resources
- Your organization already has a **LocalStack Team/Enterprise license** and the cost is not a concern

For everyone else — local dev, CI pipelines, integration tests, AI agent sandboxes — Robotocore gives you more services, more fidelity, and zero licensing friction.
