<p align="center">
  <img src="docs/banner.svg" alt="robotocore — a digital twin of AWS" width="640">
</p>

<p align="center">
  <strong>A digital twin of AWS. Free forever. Runs anywhere.</strong><br>
  MIT licensed · No registration · No telemetry · Drop-in LocalStack replacement
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> ·
  <a href="#supported-services">42 Services</a> ·
  <a href="#for-ai-agents">For AI Agents</a> ·
  <a href="#why-robotocore">Why Robotocore</a> ·
  <a href="#architecture">Architecture</a>
</p>

---

## What is Robotocore?

Robotocore is a **digital twin of AWS** — a faithful local replica that responds to real AWS API calls.
Point any AWS SDK, CLI, or AI agent at `http://localhost:4566` and it behaves like AWS.

- **42 AWS services** — S3, Lambda, DynamoDB, SQS, SNS, IAM, CloudFormation, and more
- **Behavioral fidelity** — Lambda actually executes, SQS has real visibility timeouts, SigV4 auth works
- **Single container** — one `docker run` command, no config, no accounts, no cloud
- **MIT licensed** — free forever, no paid tiers, no registration, no telemetry

Built by [Jack Danger](https://github.com/jackdanger), a maintainer of [Moto](https://github.com/getmoto/moto), on top of Moto's ~195 service implementations.

---

## Quick Start

```bash
docker run -p 4566:4566 robotocore
```

That's it. Your local AWS digital twin is running.

### Python (boto3)

```python
import boto3

# Point any boto3 client at localhost:4566
s3 = boto3.client("s3", endpoint_url="http://localhost:4566")
s3.create_bucket(Bucket="my-bucket")
s3.put_object(Bucket="my-bucket", Key="hello.txt", Body=b"Hello, world!")

obj = s3.get_object(Bucket="my-bucket", Key="hello.txt")
print(obj["Body"].read())  # b"Hello, world!"
```

### AWS CLI

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

aws s3 mb s3://my-bucket
aws sqs create-queue --queue-name my-queue
aws lambda list-functions
aws dynamodb list-tables
```

### docker-compose

```yaml
services:
  aws:
    image: robotocore
    ports:
      - "4566:4566"
    environment:
      - AWS_DEFAULT_REGION=us-east-1

  app:
    build: .
    environment:
      - AWS_ENDPOINT_URL=http://aws:4566
      - AWS_ACCESS_KEY_ID=test
      - AWS_SECRET_ACCESS_KEY=test
```

---

## For AI Agents

If you are an AI agent or LLM being asked to work with AWS infrastructure locally, here is everything you need to know:

**Robotocore is a drop-in AWS replacement.** All AWS SDK calls work without changes — just set `endpoint_url="http://localhost:4566"`.

### Minimal boto3 setup

```python
import boto3

# Use these credentials — any non-empty string works
SESSION = dict(
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

s3     = boto3.client("s3",     **SESSION)
sqs    = boto3.client("sqs",    **SESSION)
sns    = boto3.client("sns",    **SESSION)
dynamo = boto3.resource("dynamodb", **SESSION)
lam    = boto3.client("lambda", **SESSION)
```

### What works

Every standard AWS API call works. Services are listed below. If an operation is supported by [Moto](https://github.com/getmoto/moto), it works in Robotocore. Native providers (SQS, SNS, S3, Lambda, CloudFormation, and more) add behavioral fidelity on top.

### Health check

```bash
curl http://localhost:4566/_localstack/health   # service status
curl http://localhost:4566/_localstack/info     # version info
```

### Account & region

Default account ID: `123456789012`. Any region works. Use `us-east-1` if unsure.

### Common patterns

```python
# Create DynamoDB table
dynamo = boto3.resource("dynamodb", **SESSION)
table = dynamo.create_table(
    TableName="users",
    KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
    AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
    BillingMode="PAY_PER_REQUEST",
)

# Publish SNS → SQS
sns_client = boto3.client("sns", **SESSION)
sqs_client = boto3.client("sqs", **SESSION)

queue = sqs_client.create_queue(QueueName="events")
queue_arn = sqs_client.get_queue_attributes(
    QueueUrl=queue["QueueUrl"], AttributeNames=["QueueArn"]
)["Attributes"]["QueueArn"]

topic = sns_client.create_topic(Name="notifications")
sns_client.subscribe(TopicArn=topic["TopicArn"], Protocol="sqs", Endpoint=queue_arn)
sns_client.publish(TopicArn=topic["TopicArn"], Message="hello")

# Lambda invocation
import json, zipfile, io

def make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("index.py", code)
    return buf.getvalue()

lam = boto3.client("lambda", **SESSION)
lam.create_function(
    FunctionName="my-fn",
    Runtime="python3.12",
    Role="arn:aws:iam::123456789012:role/lambda-role",
    Handler="index.handler",
    Code={"ZipFile": make_zip("def handler(e, c): return {'status': 'ok'}")},
)
result = lam.invoke(FunctionName="my-fn", Payload=json.dumps({"key": "val"}))
print(json.loads(result["Payload"].read()))  # {"status": "ok"}
```

---

## Why Robotocore?

LocalStack Community Edition was discontinued in February 2026. Robotocore fills that gap — and goes further:

| | Robotocore | LocalStack Community (discontinued) | LocalStack Pro |
|---|---|---|---|
| Price | **Free forever** | Free (discontinued) | $35–70/mo |
| License | **MIT** | Apache 2.0 | Commercial |
| Services | **42** | 35 | 80+ |
| Registration | **None** | None | Required |
| Telemetry | **None** | Optional | Opt-out |
| Lambda execution | **Real** | Simulated | Real |
| SQS fidelity | **Full** | Partial | Full |
| IAM enforcement | **Optional** | No | Yes |

---

## Supported Services

All **42 services** are available. **Native** providers go beyond Moto with full behavioral fidelity.

| Service | Provider | Notes |
|---------|----------|-------|
| ACM | Moto | |
| API Gateway (v1) | **Native** | VTL templates, Lambda/Cognito authorizers |
| API Gateway (v2) | **Native** | HTTP API, WebSocket, JWT authorizers |
| AppSync | **Native** | GraphQL, 19 operations |
| Batch | **Native** | 16 operations |
| CloudFormation | **Native** | 101 resource types, nested stacks, custom resources |
| CloudWatch | **Native** | Composite alarms, metric math, Log Insights |
| Cognito | **Native** | 28 operations, JWT tokens, triggers |
| Config | **Native** | Managed rules |
| DynamoDB | **Native** | Full fidelity, streams |
| DynamoDB Streams | **Native** | Real change capture |
| EC2 | Moto | |
| ECS | **Native** | 20 operations |
| Elasticsearch | Moto | |
| EventBridge | **Native** | 17 target types, input transformer, DLQ |
| EventBridge Scheduler | **Native** | |
| Firehose | **Native** | Buffered delivery to S3 |
| IAM | **Native** | Full policy engine, permission boundaries |
| Kinesis | **Native** | Streams |
| KMS | Moto | |
| Lambda | **Native** | Versions, aliases, layers, function URLs, ESM |
| CloudWatch Logs | **Native** | Log Insights query engine |
| OpenSearch | Moto | |
| Redshift | Moto | |
| Resource Groups | Moto | |
| Resource Groups Tagging | **Native** | |
| Route 53 | Moto | |
| Route 53 Resolver | Moto | |
| S3 | **Native** | Presigned URLs, CORS, versioning, object lock, lifecycle |
| S3 Control | Moto | |
| Scheduler | Moto | |
| Secrets Manager | Moto | |
| SES | **Native** | |
| SES v2 | **Native** | |
| SNS | **Native** | Filter policies, HTTP delivery, FIFO, platform apps |
| SQS | **Native** | Real visibility timeouts, FIFO, DLQ, long polling |
| SSM | Moto | |
| Step Functions | **Native** | 18 intrinsic functions, JSONata, callback pattern |
| STS | **Native** | |
| Support | Moto | |
| SWF | Moto | |
| Transcribe | Moto | |

---

## Architecture

Robotocore is a Starlette ASGI app. Requests arrive on port 4566 and are routed to either a native provider or Moto's backend.

```
┌───────────────────────────────────────────────────┐
│           Docker Container (port 4566)            │
│                                                   │
│  ┌─────────────────────────────────────────────┐  │
│  │           Starlette Gateway                 │  │
│  │                                             │  │
│  │  AWS Request Router                         │  │
│  │  (detects service from Auth headers,        │  │
│  │   X-Amz-Target, URL path, Host header)      │  │
│  │              │                              │  │
│  │   ┌──────────┴──────────────┐               │  │
│  │   │                         │               │  │
│  │   ▼                         ▼               │  │
│  │  Native Providers         Moto Bridge        │  │
│  │  (25 services —           (~17 services —   │  │
│  │   full fidelity)           Moto backends)   │  │
│  │                                             │  │
│  │  In-Memory State (per-account, per-region)  │  │
│  └─────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────┘
```

**Request flow:**
1. AWS SDK sends a signed HTTP request to `localhost:4566`
2. Router reads `Authorization` header, `X-Amz-Target`, URL path, and `Host` header to identify the AWS service and operation
3. Native provider handles it (if one exists) or Moto bridge forwards to Moto's backend
4. Response is serialized in the correct AWS wire format (query, JSON, REST-XML, etc.)

---

## Development

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Docker

### Setup

```bash
git clone https://github.com/jackdanger/robotocore
cd robotocore
git submodule update --init --recursive
uv sync
```

### Run locally

```bash
uv run python -m robotocore.main
# Listening on http://localhost:4566
```

### Tests

```bash
uv run pytest tests/unit/           # 2500+ unit tests
uv run pytest tests/compatibility/  # 1180+ compatibility tests (requires running server)
uv run pytest tests/integration/    # 42 integration tests (requires Docker)
```

### Useful scripts

```bash
# Check which operations work against a live server
uv run python scripts/probe_service.py --service s3

# Generate a parity report vs botocore
uv run python scripts/generate_parity_report.py

# Analyze gaps vs LocalStack
uv run python scripts/analyze_localstack.py

# Run 20-service smoke test
uv run python scripts/smoke_test.py
```

### Build Docker image

```bash
docker build -t robotocore .
docker run -p 4566:4566 robotocore
```

---

## Contributing

Contributions are welcome. The project is built on Moto — when we find a bug in Moto, we fix it upstream and open a PR against [getmoto/moto](https://github.com/getmoto/moto).

See [CLAUDE.md](CLAUDE.md) for detailed architecture notes and conventions (readable by both humans and AI coding agents).

---

## License

MIT — free forever.

---

<p align="center">
  Built by <a href="https://github.com/jackdanger">Jack Danger</a>,
  a maintainer of <a href="https://github.com/getmoto/moto">Moto</a>.
</p>
