<p align="center">
  <img src="docs/banner.svg" alt="robotocore — free & open-source AWS emulator" width="600">
</p>

<p align="center">
  <strong>Drop-in replacement for LocalStack Community Edition</strong><br>
  MIT licensed · Free forever · No registration · No telemetry
</p>

---

## Quick Start

```bash
docker run -p 4566:4566 robotocore
```

Then point any AWS SDK at `http://localhost:4566`:

```python
import boto3

s3 = boto3.client("s3", endpoint_url="http://localhost:4566")
s3.create_bucket(Bucket="my-bucket")
s3.put_object(Bucket="my-bucket", Key="hello.txt", Body=b"Hello, world!")
```

```bash
# Or use the AWS CLI
aws --endpoint-url=http://localhost:4566 s3 ls
aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name my-queue
```

## Why Robotocore?

LocalStack Community Edition was discontinued in February 2026. Robotocore fills that gap:

- **Free forever** — MIT license, no registration, no paid tiers, no telemetry
- **Drop-in replacement** — Same port (4566), same request routing, same response format
- **35 AWS services** — Full parity with LocalStack Community, proven by 342 tests
- **Single container** — One `docker run` command, works on ARM Mac and x86

## Supported Services

All **35 services** from LocalStack Community Edition are supported:

| Service | Provider | Service | Provider |
|---------|----------|---------|----------|
| ACM | Moto | Lambda | Moto |
| API Gateway | Moto | Logs | Moto |
| **CloudFormation** | **Native** | OpenSearch | Moto |
| CloudWatch | Moto | Redshift | Moto |
| Config | Moto | Resource Groups | Moto |
| DynamoDB | Moto | Resource Groups Tagging | Moto |
| DynamoDB Streams | Moto | Route 53 | Moto |
| EC2 | Moto | Route 53 Resolver | Moto |
| Elasticsearch | Moto | **S3** | **Native** |
| EventBridge | Moto | S3 Control | Moto |
| **Firehose** | **Native** | Scheduler | Moto |
| IAM | Moto | Secrets Manager | Moto |
| Kinesis | Moto | SES | Moto |
| KMS | Moto | **SNS** | **Native** |
| **SQS** | **Native** | SSM | Moto |
| Step Functions | Moto | STS | Moto |
| Support | Moto | SWF | Moto |
| Transcribe | Moto | | |

**Native providers** go beyond Moto with behavioral fidelity:
- **SQS** — Real visibility timeouts, FIFO ordering, dead-letter queues, long polling
- **SNS** — Cross-service delivery (SNS → SQS), message filtering
- **S3** — Event notifications to SQS/SNS with prefix/suffix filters
- **Firehose** — Buffered delivery streams to S3
- **CloudFormation** — Template engine with 12 resource types, intrinsic functions, dependency ordering

## Development

```bash
# Prerequisites: Python 3.12+, uv
git clone https://github.com/jackdanger/robotocore
cd robotocore
git submodule update --init --recursive
uv sync

# Run locally
uv run python -m robotocore.main

# Run tests
uv run pytest tests/unit/           # 116 unit tests
uv run pytest tests/compatibility/  # 226 compatibility tests

# Build Docker image
docker build -t robotocore .

# Generate parity report
uv run python scripts/parity_report.py
```

## Architecture

```
┌──────────────────────────────────────────────┐
│             Docker Container (4566)           │
│                                               │
│  ┌─────────────────────────────────────────┐  │
│  │          Starlette Gateway              │  │
│  │  ┌───────────────────────────────────┐  │  │
│  │  │   Request Router                  │  │  │
│  │  │   (Auth headers, X-Amz-Target,    │  │  │
│  │  │    URL paths, Host header)        │  │  │
│  │  └──────────┬────────────────────────┘  │  │
│  │             │                           │  │
│  │  ┌──────────▼──────────┐ ┌───────────┐ │  │
│  │  │  Native Providers   │ │   Moto    │ │  │
│  │  │  SQS, SNS, S3,     │ │  Bridge   │ │  │
│  │  │  Firehose, CFN     │ │  (30 svcs)│ │  │
│  │  └─────────────────────┘ └───────────┘ │  │
│  └─────────────────────────────────────────┘  │
└──────────────────────────────────────────────┘
```

## License

MIT — free forever.
