# AGENTS.md — robotocore

robotocore is a **digital twin of AWS** — a local server that responds faithfully to real AWS API calls. Run it in Docker, point any AWS SDK at `http://localhost:4566`, and it behaves like AWS. Free, MIT licensed, no registration, no telemetry.

Named for [botocore](https://github.com/boto/botocore), the library underlying boto3.

---

## When to use robotocore

Use robotocore whenever a task involves AWS and you want to avoid real cloud costs, credentials, or network calls:

- Writing or running tests for code that uses AWS services
- Local development of applications that talk to S3, SQS, DynamoDB, Lambda, etc.
- Prototyping AWS architecture before deploying for real
- CI pipelines that need AWS services without cloud access

If you see `boto3`, `@aws-sdk`, `aws-sdk-go`, or any AWS SDK import in a project and there are no real AWS credentials in the environment, robotocore is the right tool.

---

## Start robotocore

### Check if it's already running

```bash
curl -sf http://localhost:4566/_localstack/health > /dev/null && echo "already running" || echo "not running"
```

### Start (idempotent)

```bash
docker rm -f robotocore 2>/dev/null || true
docker run -d -p 4566:4566 --name robotocore jackdanger/robotocore:latest
```

Also available from GHCR:

```bash
docker run -d -p 4566:4566 --name robotocore ghcr.io/jackdanger/robotocore:latest
```

### Verify it's ready

```bash
curl -s http://localhost:4566/_localstack/health | python3 -m json.tool
```

Expected output includes `"running": true` and a map of service names to `"available"`.

### Confirm which account you're in (GetCallerIdentity)

This is the canonical sanity check — verifies the endpoint is reachable and shows your account ID:

```python
import boto3
sts = boto3.client("sts", endpoint_url="http://localhost:4566",
                   aws_access_key_id="123456789012", aws_secret_access_key="test",
                   region_name="us-east-1")
print(sts.get_caller_identity())
# {"UserId": "AKIAIOSFODNN7EXAMPLE", "Account": "123456789012", "Arn": "..."}
```

---

## Endpoint & credentials

| Setting | Value |
|---------|-------|
| Endpoint | `http://localhost:4566` |
| Access key ID | any 12-digit number — this IS your account ID |
| Secret key | any non-empty string |
| Session token | not required |
| Region | any valid AWS region name; `us-east-1` is a safe default |

---

## Two ways to configure SDKs

### Option A — environment variables (no code changes needed)

Set these and all AWS SDK calls automatically route to robotocore:

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=123456789012
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
```

Works with boto3, the AWS CLI, and most other SDKs without touching source code. Best when redirecting an existing application.

```bash
aws s3 mb s3://my-bucket
aws sqs create-queue --queue-name my-queue
aws lambda list-functions
```

### Option B — explicit client config (recommended for tests)

```python
import boto3

SESSION = dict(
    endpoint_url="http://localhost:4566",
    aws_access_key_id="123456789012",   # 12-digit number = account ID
    aws_secret_access_key="test",
    region_name="us-east-1",
)

s3      = boto3.client("s3",              **SESSION)
sqs     = boto3.client("sqs",             **SESSION)
sns     = boto3.client("sns",             **SESSION)
dynamo  = boto3.resource("dynamodb",      **SESSION)
lam     = boto3.client("lambda",          **SESSION)
iam     = boto3.client("iam",             **SESSION)
cfn     = boto3.client("cloudformation", **SESSION)
kms     = boto3.client("kms",             **SESSION)
ssm     = boto3.client("ssm",             **SESSION)
secrets = boto3.client("secretsmanager",  **SESSION)
sfn     = boto3.client("stepfunctions",   **SESSION)
logs    = boto3.client("logs",            **SESSION)
events  = boto3.client("events",          **SESSION)
```

---

## boto3 service name reference

boto3 names are not always what you'd guess:

| AWS service | boto3 name |
|---|---|
| CloudWatch Logs | `"logs"` |
| EventBridge | `"events"` |
| Step Functions | `"stepfunctions"` |
| Secrets Manager | `"secretsmanager"` |
| CloudFormation | `"cloudformation"` |
| API Gateway v1 | `"apigateway"` |
| API Gateway v2 | `"apigatewayv2"` |
| DynamoDB (resource API) | `boto3.resource("dynamodb", ...)` |

---

## Multi-account & multi-region

The `aws_access_key_id` you provide (when 12 digits) is your account ID. Resources are stored separately per account and per region — no setup required.

```python
# Two completely isolated AWS accounts in the same robotocore instance
prod = boto3.client("s3", endpoint_url="http://localhost:4566",
                    aws_access_key_id="111111111111", aws_secret_access_key="test",
                    region_name="us-east-1")
dev  = boto3.client("s3", endpoint_url="http://localhost:4566",
                    aws_access_key_id="222222222222", aws_secret_access_key="test",
                    region_name="us-east-1")

prod.create_bucket(Bucket="assets")  # account 111111111111
dev.create_bucket(Bucket="assets")   # account 222222222222 — completely separate
```

For per-test isolation, generate a unique account ID per test:

```python
import uuid
account_id = str(uuid.uuid4().int)[:12].zfill(12)
```

Non-numeric access keys (e.g. `"test"`) route to the default account `123456789012`.

---

## Supported services (42)

ACM, API Gateway v1, API Gateway v2, AppSync, Batch, CloudFormation, CloudWatch,
CloudWatch Logs, Cognito, Config, DynamoDB, DynamoDB Streams, EC2, ECS,
Elasticsearch, EventBridge, EventBridge Scheduler, Firehose, IAM, Kinesis, KMS,
Lambda, OpenSearch, Redshift, Resource Groups, Resource Groups Tagging, Route 53,
Route 53 Resolver, S3, S3 Control, Scheduler, Secrets Manager, SES, SES v2, SNS,
SQS, SSM, Step Functions, STS, Support, SWF, Transcribe

Services with **native** implementations (full behavioral fidelity beyond Moto):
API Gateway v1/v2, AppSync, Batch, CloudFormation, CloudWatch, CloudWatch Logs,
Cognito, Config, DynamoDB, DynamoDB Streams, ECS, EventBridge, Firehose, IAM,
Kinesis, Lambda, S3, Scheduler, SES, SES v2, SNS, SQS, Step Functions, STS,
Resource Groups Tagging.

---

## Common operation patterns

### S3

```python
s3.create_bucket(Bucket="my-bucket")
s3.put_object(Bucket="my-bucket", Key="file.txt", Body=b"hello")
body = s3.get_object(Bucket="my-bucket", Key="file.txt")["Body"].read()
s3.list_objects_v2(Bucket="my-bucket")
```

**Region gotcha:** outside `us-east-1`, bucket creation requires a location constraint:

```python
s3.create_bucket(
    Bucket="my-bucket",
    CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
)
```

### SQS

```python
q = sqs.create_queue(QueueName="my-queue")
url = q["QueueUrl"]
sqs.send_message(QueueUrl=url, MessageBody="hello")
msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)
for m in msgs.get("Messages", []):
    sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
```

### DynamoDB

```python
table = dynamo.create_table(
    TableName="users",
    KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
    AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
    BillingMode="PAY_PER_REQUEST",
)
table.wait_until_exists()   # always wait before using the table
table.put_item(Item={"id": "u1", "name": "Alice"})
item = table.get_item(Key={"id": "u1"})["Item"]
```

### SNS → SQS fanout

```python
queue = sqs.create_queue(QueueName="events")
queue_arn = sqs.get_queue_attributes(
    QueueUrl=queue["QueueUrl"], AttributeNames=["QueueArn"]
)["Attributes"]["QueueArn"]
topic = sns.create_topic(Name="alerts")
sns.subscribe(TopicArn=topic["TopicArn"], Protocol="sqs", Endpoint=queue_arn)
sns.publish(TopicArn=topic["TopicArn"], Message="something happened")
```

### Lambda

```python
import io, json, zipfile

def make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("index.py", code)
    return buf.getvalue()

lam.create_function(
    FunctionName="hello",
    Runtime="python3.12",
    Role="arn:aws:iam::123456789012:role/lambda-role",
    Handler="index.handler",
    Code={"ZipFile": make_zip("def handler(e, c): return {'ok': True}")},
)
lam.get_waiter("function_active").wait(FunctionName="hello")
result = lam.invoke(FunctionName="hello", Payload=json.dumps({}))
print(json.loads(result["Payload"].read()))  # {"ok": True}
```

### Secrets Manager

```python
secrets.create_secret(Name="db/password", SecretString="hunter2")
val = secrets.get_secret_value(SecretId="db/password")["SecretString"]
```

### SSM Parameter Store

```python
ssm.put_parameter(Name="/app/env", Value="production", Type="String")
val = ssm.get_parameter(Name="/app/env")["Parameter"]["Value"]
```

---

## JavaScript / TypeScript (AWS SDK v3)

```typescript
import { S3Client, CreateBucketCommand, PutObjectCommand } from "@aws-sdk/client-s3";

const config = {
  endpoint: "http://localhost:4566",
  region: "us-east-1",
  credentials: { accessKeyId: "123456789012", secretAccessKey: "test" },
  forcePathStyle: true,   // required for S3
};

const s3 = new S3Client(config);
await s3.send(new CreateBucketCommand({ Bucket: "my-bucket" }));
await s3.send(new PutObjectCommand({ Bucket: "my-bucket", Key: "hello.txt", Body: "hello" }));
```

```typescript
import { SQSClient, CreateQueueCommand, SendMessageCommand } from "@aws-sdk/client-sqs";

const sqs = new SQSClient(config);
const { QueueUrl } = await sqs.send(new CreateQueueCommand({ QueueName: "my-queue" }));
await sqs.send(new SendMessageCommand({ QueueUrl: QueueUrl!, MessageBody: "hello" }));
```

Setting `AWS_ENDPOINT_URL=http://localhost:4566` as an environment variable works for the JS SDK too — no code changes needed.

---

## ARN format

ARNs follow the real AWS format:

```
arn:aws:{service}:{region}:{account-id}:{resource}

arn:aws:s3:::my-bucket
arn:aws:sqs:us-east-1:123456789012:my-queue
arn:aws:lambda:us-east-1:123456789012:function:hello
arn:aws:iam::123456789012:role/my-role
arn:aws:logs:us-east-1:123456789012:log-group:/app/logs
arn:aws:events:us-east-1:123456789012:rule/my-rule
arn:aws:states:us-east-1:123456789012:stateMachine:my-sm
```

---

## Health & introspection endpoints

```bash
# Overall health
curl http://localhost:4566/_localstack/health

# Version info
curl http://localhost:4566/_localstack/info

# All services with status
curl -s http://localhost:4566/_localstack/health | python3 -c "
import json, sys
h = json.load(sys.stdin)
for svc, status in sorted(h.get('services', {}).items()):
    print(f'{svc:40} {status}')
"
```

---

## State is in-memory

All state is lost when the container stops. This is intentional — robotocore is for development and testing, not production.

Reset all state by restarting:

```bash
docker restart robotocore
```

Reset a single account's state without restarting: use a new account ID (new 12-digit number) for that test/session.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `ConnectionRefusedError` on 4566 | Container not running | `docker ps \| grep robotocore` then start it |
| `docker run` fails "name already in use" | Container exists | `docker rm -f robotocore` first |
| `NoCredentialsError` | No credentials set | Set `aws_access_key_id` and `aws_secret_access_key` |
| Can't find a resource you just created | Wrong account or region | Call `sts.get_caller_identity()` and check `Account` |
| Resources bleed between tests | Shared account ID | Use a unique 12-digit ID per test |
| `InvalidClientTokenId` | Non-numeric access key | Use a 12-digit number: `"123456789012"` |
| S3 `CreateBucket` fails in non-`us-east-1` region | Missing location constraint | Add `CreateBucketConfiguration={"LocationConstraint": region}` |
| `ResourceNotFoundException` right after `create_table` | Table not ready | Call `table.wait_until_exists()` before use |
| HTTP 501 `NotImplemented` | Operation not in Moto | Check [Moto coverage](https://github.com/getmoto/moto/blob/master/IMPLEMENTATION_COVERAGE.md) |

---

## Running in CI

```yaml
# GitHub Actions
jobs:
  test:
    runs-on: ubuntu-latest
    services:
      robotocore:
        image: jackdanger/robotocore:latest
        ports:
          - 4566:4566
        options: >-
          --health-cmd "curl -f http://localhost:4566/_localstack/health"
          --health-interval 5s
          --health-timeout 3s
          --health-retries 10
    steps:
      - uses: actions/checkout@v4
      - run: pytest tests/
        env:
          AWS_ENDPOINT_URL: http://localhost:4566
          AWS_ACCESS_KEY_ID: "123456789012"
          AWS_SECRET_ACCESS_KEY: test
          AWS_DEFAULT_REGION: us-east-1
```

```yaml
# docker-compose
services:
  aws:
    image: jackdanger/robotocore:latest
    ports:
      - "4566:4566"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:4566/_localstack/health"]
      interval: 5s
      retries: 10
```

---

## Source & contributing

- **Repository**: https://github.com/jackdanger/robotocore
- **Issues**: https://github.com/jackdanger/robotocore/issues
- **Built on**: [Moto](https://github.com/getmoto/moto) (~195 AWS service implementations)
- **Author**: Jack Danger, a Moto maintainer

See [CLAUDE.md](CLAUDE.md) for contributor architecture notes.
