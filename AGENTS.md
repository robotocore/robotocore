# AGENTS.md — robotocore

robotocore is a **digital twin of AWS** — a local server that responds faithfully to real AWS API calls. Run it in Docker, point any AWS SDK at `http://localhost:4566`, and it behaves like AWS. Free, MIT licensed, no registration, no telemetry.

Named for [botocore](https://github.com/boto/botocore), the library underlying boto3.

---

## Start robotocore

```bash
docker run -d -p 4566:4566 --name robotocore jackdanger/robotocore:latest
```

Also available from GHCR:

```bash
docker run -d -p 4566:4566 --name robotocore ghcr.io/jackdanger/robotocore:latest
```

Verify it's running:

```bash
curl -s http://localhost:4566/_localstack/health | python3 -m json.tool
```

Expected output includes `"running": true` and a list of services.

---

## Endpoint & credentials

| Setting | Value |
|---------|-------|
| Endpoint | `http://localhost:4566` |
| Access key ID | any 12-digit number (becomes your account ID) |
| Secret key | any non-empty string |
| Session token | not required |
| Region | any valid AWS region; default `us-east-1` |

---

## Minimal Python setup

```python
import boto3

# Replace 123456789012 with any 12-digit number to choose your account ID
SESSION = dict(
    endpoint_url="http://localhost:4566",
    aws_access_key_id="123456789012",
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
```

---

## Multi-account & multi-region

The `aws_access_key_id` you provide (when it's a 12-digit number) is your account ID. Resources are stored separately per account and per region.

```python
# Two completely isolated AWS accounts in the same robotocore instance
prod = boto3.client("s3", endpoint_url="http://localhost:4566",
                    aws_access_key_id="111111111111", aws_secret_access_key="test",
                    region_name="us-east-1")
dev  = boto3.client("s3", endpoint_url="http://localhost:4566",
                    aws_access_key_id="222222222222", aws_secret_access_key="test",
                    region_name="us-east-1")

prod.create_bucket(Bucket="assets")  # in account 111111111111
dev.create_bucket(Bucket="assets")   # separate bucket in account 222222222222
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

All standard SDK operations work. Services marked **Native** in the README have
behavioral fidelity beyond Moto (real Lambda execution, real SQS visibility
timeouts, full IAM policy evaluation, etc.).

---

## Common operation patterns

### S3

```python
s3.create_bucket(Bucket="my-bucket")
s3.put_object(Bucket="my-bucket", Key="file.txt", Body=b"hello")
body = s3.get_object(Bucket="my-bucket", Key="file.txt")["Body"].read()
s3.list_objects_v2(Bucket="my-bucket")
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

## ARN format

ARNs follow the real AWS format:

```
arn:aws:{service}:{region}:{account-id}:{resource}

# Examples
arn:aws:s3:::my-bucket
arn:aws:sqs:us-east-1:123456789012:my-queue
arn:aws:lambda:us-east-1:123456789012:function:hello
arn:aws:iam::123456789012:role/my-role
```

---

## Health & introspection endpoints

```bash
# Overall health + running services
curl http://localhost:4566/_localstack/health

# Version and build info
curl http://localhost:4566/_localstack/info

# Full service list
curl http://localhost:4566/_localstack/health | python3 -c "
import json,sys; h=json.load(sys.stdin)
for svc,status in sorted(h.get('services',{}).items()):
    print(f'{svc:40} {status}')
"
```

---

## State is in-memory

All state lives in memory and is lost when the container stops. There is no
persistence layer by default. This is intentional — robotocore is for development
and testing, not production.

To reset all state, restart the container:

```bash
docker restart robotocore
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `ConnectionRefused` on 4566 | Container not running | `docker ps`, then `docker run ...` |
| `NoCredentialsError` | boto3 has no credentials at all | Set `aws_access_key_id` and `aws_secret_access_key` |
| Resources from different tests bleed together | Same account ID used across tests | Use a unique 12-digit account ID per test (see multi-account section) |
| `InvalidClientTokenId` | Non-12-digit access key used as account | Use a 12-digit number like `"123456789012"` |
| Operation returns `NotImplemented` | Moto doesn't implement it yet | Check [Moto coverage](https://github.com/getmoto/moto/blob/master/IMPLEMENTATION_COVERAGE.md) |

---

## Running in CI

```yaml
# GitHub Actions example
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
```

```yaml
# docker-compose for local dev
services:
  aws:
    image: jackdanger/robotocore:latest
    ports:
      - "4566:4566"
    environment:
      - AWS_DEFAULT_REGION=us-east-1
```

---

## Source & contributing

- **Repository**: https://github.com/jackdanger/robotocore
- **Issues**: https://github.com/jackdanger/robotocore/issues
- **Built on**: [Moto](https://github.com/getmoto/moto) (~195 AWS service implementations)
- **Author**: Jack Danger, a Moto maintainer

See [CLAUDE.md](CLAUDE.md) for contributor architecture notes.
