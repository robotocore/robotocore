# Migrating to Robotocore

A step-by-step guide for teams migrating from **LocalStack Community Edition** or **moto standalone server** to Robotocore.

---

## Why Migrate

### LocalStack Community Edition is being sunset

LocalStack has announced that **Community Edition support ends March 23, 2026**. The project is consolidating to a single Docker image that requires an auth token. A free tier continues for non-commercial use, students, and open-source projects, but the unrestricted Community image (`localstack/localstack`) will no longer be maintained.

If your CI pipelines or dev environments depend on the Community image without an auth token, they will stop receiving updates and eventually break as AWS APIs evolve.

### moto standalone server has no behavioral fidelity

The moto server (`moto_server` / `motoserver/moto`) exposes Moto's mock backends over HTTP. It covers ~195 services but every service is a pure mock:

- Lambda does not execute function code
- SQS has no real visibility timeouts or message retention
- DynamoDB Streams does not emit change records
- No infrastructure features (chaos engineering, audit logs, state snapshots)
- No plugin/extension system
- Not optimized for container deployment

### Robotocore gives you both breadth and fidelity

Robotocore is MIT-licensed, requires no registration or auth tokens, and will remain free forever. It runs on port 4566 (same as LocalStack), serves 147 AWS services, and adds behavioral fidelity where it matters: Lambda actually executes, SQS has real visibility timeouts, CloudWatch alarms evaluate, and DynamoDB Streams emit change records.

---

## Quick Start

### From LocalStack Community

```bash
# Before (LocalStack Community)
docker run -p 4566:4566 localstack/localstack

# After (Robotocore) -- same port, drop-in replacement
docker run -p 4566:4566 robotocore/robotocore
```

No client-side changes needed. Your `--endpoint-url http://localhost:4566` configuration works as-is.

### From moto standalone server

```bash
# Before (moto server on port 5000)
docker run -p 5000:5000 motoserver/moto

# After (Robotocore on port 4566, or map to 5000 if you prefer)
docker run -p 4566:4566 robotocore/robotocore

# Or, to keep your existing port:
docker run -p 5000:4566 robotocore/robotocore
```

Update your `--endpoint-url` from `http://localhost:5000` to `http://localhost:4566` (or use the port mapping above to avoid changing clients).

---

## Service Coverage Comparison

### Core services (LocalStack Community's ~25)

Every service available in LocalStack Community Edition is available in Robotocore. Most have native providers with enhanced behavioral fidelity.

| Service | LocalStack Community | moto server | Robotocore | Notes |
|---------|---------------------|-------------|------------|-------|
| ACM | Yes | Yes | Yes (native) | |
| API Gateway | Yes | Yes | Yes (native) | REST + HTTP APIs |
| API Gateway V2 | No | Yes | Yes (native) | WebSocket + HTTP APIs |
| AppSync | No | Yes | Yes (native) | |
| Batch | No | Yes | Yes (native) | |
| CloudFormation | Yes | Yes | Yes (native) | 58 resource types |
| CloudWatch | Yes | Yes | Yes (native) | Alarm evaluation |
| CloudWatch Logs | Yes | Yes | Yes (native) | |
| Cognito (IdP) | No | Yes | Yes (native) | |
| Config | No | Yes | Yes (native) | |
| DynamoDB | Yes | Yes | Yes (native) | Stream mutation hooks |
| DynamoDB Streams | Yes | Yes | Yes (native) | Real change records |
| EC2 | Yes | Yes | Yes (native) | |
| ECR | No | Yes | Yes (native) | |
| ECS | No | Yes | Yes (native) | |
| Elasticsearch | Yes | Yes | Yes (native) | |
| EventBridge | Yes | Yes | Yes (native) | 17 target types |
| Firehose | Yes | Yes | Yes (native) | |
| IAM | Yes | Yes | Yes (native) | Opt-in enforcement |
| Kinesis | Yes | Yes | Yes (native) | |
| KMS | Yes | Yes | Yes (Moto-backed) | |
| Lambda | Yes | Yes | Yes (native) | Actually executes code |
| OpenSearch | No | Yes | Yes (native) | |
| Pipes | No | Yes | Yes (native) | |
| Redshift | Yes | Yes | Yes (Moto-backed) | |
| Rekognition | No | Yes | Yes (native) | |
| Route53 | Yes | Yes | Yes (native) | |
| S3 | Yes | Yes | Yes (native) | Event notifications |
| Scheduler | No | Yes | Yes (native) | |
| SecretsManager | Yes | Yes | Yes (native) | |
| SES | Yes | Yes | Yes (native) | |
| SES V2 | No | Yes | Yes (native) | |
| SNS | Yes | Yes | Yes (native) | Cross-service delivery |
| SQS | Yes | Yes | Yes (native) | Real visibility timeouts |
| SSM | Yes | Yes | Yes (native) | |
| StepFunctions | Yes | Yes | Yes (native) | |
| STS | Yes | Yes | Yes (native) | |
| Support | No | Yes | Yes (native) | |
| X-Ray | No | Yes | Yes (native) | |

### Additional services (101 Moto-backed)

Beyond the 46 native providers listed above, Robotocore registers 101 additional services via Moto backends. These include: Athena, Auto Scaling, CodeBuild, CodeCommit, CodePipeline, Glue, IoT, MediaStore, MQ, Neptune, QLDB, RDS, Redshift, SageMaker, WAF, and many more. Run `aws --endpoint-url http://localhost:4566 <service> help` to check any specific service.

**Total: 147 services** (46 native + 101 Moto-backed), compared to ~25 for LocalStack Community and ~195 for moto server (though moto's implementations are pure mocks with no behavioral fidelity).

---

## Configuration Mapping

### LocalStack env vars to Robotocore equivalents

| LocalStack env var | Robotocore equivalent | Notes |
|---|---|---|
| `GATEWAY_LISTEN=:4566` | Default is 4566 | Same default port |
| `SERVICES=s3,sqs,lambda` | All 147 services always available | No need to specify; all services are on |
| `DEBUG=1` | `ROBOTOCORE_LOG_LEVEL=DEBUG` | Structured logging with levels |
| `PERSISTENCE=1` | State snapshots (see below) | LocalStack persistence was Pro-only anyway |
| `LAMBDA_EXECUTOR=local` | Default behavior | Lambda executes in-process by default |
| `LAMBDA_REMOTE_DOCKER=0` | Not needed | No Docker-in-Docker requirement |
| `DEFAULT_REGION=us-east-1` | `AWS_DEFAULT_REGION=us-east-1` | Standard AWS env var |
| `ENFORCE_IAM=1` | `ENFORCE_IAM=1` | Same env var name |
| `LOCALSTACK_API_KEY=...` | Not needed | No auth tokens, no registration |

### moto server flags to Robotocore equivalents

| moto server | Robotocore | Notes |
|---|---|---|
| `moto_server -p 5000` | `docker run -p 4566:4566 robotocore/robotocore` | Port is configurable via Docker mapping |
| `moto_server -H 0.0.0.0` | Default binds to `0.0.0.0` inside container | |
| `POST /moto-api/reset` | `POST /_robotocore/state/reset` | State reset endpoint |
| `POST /moto-api/state-manager` | `POST /_robotocore/state/save` | Snapshot save |
| N/A | `POST /_robotocore/state/load` | Snapshot restore (not available in moto) |

---

## CLI Migration

### From `awslocal` (LocalStack CLI wrapper)

LocalStack ships `awslocal`, which is just `aws --endpoint-url=http://localhost:4566`. You have three options:

**Option 1: Use the `aws` CLI directly (recommended)**

```bash
# Instead of:
awslocal s3 ls

# Use:
aws --endpoint-url=http://localhost:4566 s3 ls
```

**Option 2: Set the endpoint globally via env var**

```bash
export AWS_ENDPOINT_URL=http://localhost:4566

# Now plain `aws` commands hit Robotocore:
aws s3 ls
aws sqs list-queues
aws lambda list-functions
```

**Option 3: Shell alias**

```bash
alias awslocal='aws --endpoint-url=http://localhost:4566'
```

### From moto server

Same as above, but update the port from 5000 to 4566 (or use `docker run -p 5000:4566` to keep your existing port).

---

## Feature Comparison

| Feature | LocalStack Community | moto server | Robotocore |
|---------|---------------------|-------------|------------|
| **License** | Apache 2.0 (sunsetting) | Apache 2.0 | MIT |
| **Auth token required** | Yes (after March 2026) | No | No, never |
| **Service count** | ~25 | ~195 (pure mocks) | 147 (46 native + 101 Moto-backed) |
| **Lambda execution** | Yes (basic) | No (mock only) | Yes (in-process Python) |
| **SQS visibility timeouts** | Basic | No | Yes (real timeouts, retention, purge) |
| **DynamoDB Streams** | Basic | No (mock only) | Yes (real change records) |
| **CloudWatch alarm evaluation** | No (Pro) | No | Yes |
| **EventBridge target dispatch** | Basic | No | Yes (17 target types) |
| **CloudFormation** | Yes | Yes (limited) | Yes (58 resource types) |
| **IAM enforcement** | No (Pro) | No | Yes (opt-in via `ENFORCE_IAM=1`) |
| **State snapshots** | No (Pro: Cloud Pods) | No | Yes (`/_robotocore/state/save` and `/load`) |
| **Selective persistence** | No | No | Yes (per-service snapshots) |
| **Chaos engineering** | No (Pro) | No | Yes (`/_robotocore/chaos/rules`) |
| **Audit log** | No | No | Yes (`/_robotocore/audit`) |
| **Resource browser** | No (Pro) | No | Yes (`/_robotocore/resources`) |
| **Plugin/extension system** | No (Pro) | No | Yes (`RobotocorePlugin` base class) |
| **Diagnostics header** | No | No | Yes (`x-robotocore-diag` on errors) |
| **Container boot time** | ~10-15s | ~2s | <5s |
| **ARM Mac native** | Yes | Yes | Yes |

---

## State Management

### Replacing LocalStack persistence (Pro-only feature)

LocalStack Community never had persistence -- it was a Pro feature. Robotocore provides state snapshots that cover the same use case:

```bash
# Save current state
curl -X POST http://localhost:4566/_robotocore/state/save \
  -d '{"name": "my-snapshot"}'

# Restore state
curl -X POST http://localhost:4566/_robotocore/state/load \
  -d '{"name": "my-snapshot"}'

# Save only specific services
curl -X POST http://localhost:4566/_robotocore/state/save \
  -d '{"name": "db-only", "services": ["dynamodb", "s3"]}'

# Reset all state
curl -X POST http://localhost:4566/_robotocore/state/reset
```

### Replacing moto's state reset

```bash
# moto server (before)
curl -X POST http://localhost:5000/moto-api/reset

# Robotocore (after)
curl -X POST http://localhost:4566/_robotocore/state/reset
```

---

## Docker Compose Examples

### Basic (replacing LocalStack Community)

```yaml
# docker-compose.yml
services:
  robotocore:
    image: robotocore/robotocore
    ports:
      - "4566:4566"
    environment:
      - ROBOTOCORE_LOG_LEVEL=INFO
```

### With IAM enforcement and debug logging

```yaml
services:
  robotocore:
    image: robotocore/robotocore
    ports:
      - "4566:4566"
    environment:
      - ENFORCE_IAM=1
      - ROBOTOCORE_LOG_LEVEL=DEBUG
      - AUDIT_LOG_SIZE=10000
```

### Replacing moto server (keeping port 5000)

```yaml
services:
  robotocore:
    image: robotocore/robotocore
    ports:
      - "5000:4566"
```

---

## Known Differences and Caveats

### vs LocalStack Community

1. **No `localstack` CLI**. Use `aws` with `--endpoint-url` or set `AWS_ENDPOINT_URL`. There is no `localstack start/stop` equivalent; use `docker run/stop` directly.
2. **No `SERVICES` env var**. All 147 services are always available. There is no way to restrict to a subset (and no reason to -- unused services consume no resources).
3. **No health endpoint at `/_localstack/health`**. Use `GET /_robotocore/resources` or simply `aws --endpoint-url http://localhost:4566 sts get-caller-identity` as a health check.
4. **Internal API paths differ**. LocalStack uses `/_localstack/*` endpoints. Robotocore uses `/_robotocore/*`. Update any scripts that call internal APIs directly.
5. **Lambda execution model differs**. LocalStack Community runs Lambda in a subprocess or Docker container. Robotocore executes Python Lambda functions in-process for speed. Non-Python runtimes have limited support.
6. **No Edge port distinction**. LocalStack historically had separate edge/service ports. Robotocore has a single port (4566) for everything.

### vs moto server

1. **Port change**: moto defaults to 5000, Robotocore defaults to 4566. Use `-p 5000:4566` in Docker if you cannot update clients.
2. **Reset endpoint moved**: `POST /moto-api/reset` becomes `POST /_robotocore/state/reset`.
3. **Some moto services not registered**: Robotocore registers 147 of Moto's ~195 services. 11 services were deregistered because all operations returned 500 errors. The remaining ~37 are Moto services that have not been validated yet. If you need a specific unregistered service, open an issue.
4. **Behavioral differences are intentional**: Where moto returns a canned response, Robotocore may enforce constraints that real AWS enforces (e.g., SQS `PurgeQueueInProgress` error if you purge twice within 60 seconds, `QueueDeletedRecently` if you recreate a queue too fast). These are features, not bugs.
5. **Response format**: Robotocore uses botocore's own service specs for serialization. In rare cases, response XML/JSON structure may differ slightly from moto's hand-written serializers. The responses will be correct per the AWS spec.

### General notes

- **Default account ID**: `123456789012` (same as both LocalStack and moto)
- **Default region**: `us-east-1` (override with `AWS_DEFAULT_REGION`)
- **Credentials**: Any dummy credentials work. Set `AWS_ACCESS_KEY_ID=test` and `AWS_SECRET_ACCESS_KEY=test` as you would with LocalStack or moto.
- **HTTP only**: Robotocore serves HTTP on port 4566. There is no TLS termination built in. Use a reverse proxy if you need HTTPS.

---

## SDK and Terraform Configuration

### boto3 (Python)

```python
import boto3

# Option 1: Per-client
client = boto3.client("s3", endpoint_url="http://localhost:4566")

# Option 2: Environment variable (boto3 >= 1.28.57)
# export AWS_ENDPOINT_URL=http://localhost:4566
client = boto3.client("s3")  # automatically uses the env var
```

### AWS SDK for JavaScript/TypeScript

```typescript
import { S3Client } from "@aws-sdk/client-s3";

const client = new S3Client({
  endpoint: "http://localhost:4566",
  region: "us-east-1",
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
  forcePathStyle: true, // required for S3
});
```

### Terraform

```hcl
provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3             = "http://localhost:4566"
    sqs            = "http://localhost:4566"
    lambda         = "http://localhost:4566"
    dynamodb       = "http://localhost:4566"
    iam            = "http://localhost:4566"
    # ... all services use the same endpoint
  }
}
```

### AWS CDK (with cdklocal or direct)

```bash
# If you used cdklocal with LocalStack, switch to direct CDK with endpoint override:
export AWS_ENDPOINT_URL=http://localhost:4566
cdk deploy --require-approval never
```

---

## Verifying Your Migration

After switching your Docker image, run these checks to confirm everything works:

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

# 1. Basic connectivity
aws sts get-caller-identity

# 2. S3
aws s3 mb s3://test-bucket
aws s3 ls

# 3. SQS
aws sqs create-queue --queue-name test-queue
aws sqs list-queues

# 4. DynamoDB
aws dynamodb create-table \
  --table-name test-table \
  --key-schema AttributeName=id,KeyType=HASH \
  --attribute-definitions AttributeName=id,AttributeType=S \
  --billing-mode PAY_PER_REQUEST
aws dynamodb list-tables

# 5. Lambda (if used)
# Create a simple test function and invoke it

# 6. Check the resource browser
curl http://localhost:4566/_robotocore/resources
```

If any service behaves differently than expected, check the audit log for details:

```bash
curl http://localhost:4566/_robotocore/audit
```
