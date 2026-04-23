# Migrating to Robotocore SQS from ElasticMQ or goaws

## Quick Start

Replace your existing emulator container with Robotocore. No SDK code changes beyond the endpoint URL.

### From ElasticMQ

```bash
# Before (ElasticMQ)
docker run -p 9324:9324 softwaremill/elasticmq-native

# After (Robotocore)
docker run -p 4566:4566 robotocore/robotocore
```

Update your endpoint:

```
# ElasticMQ default
http://localhost:9324

# Robotocore
http://localhost:4566
```

If you cannot change the port in your application, map it:

```bash
docker run -p 9324:4566 robotocore/robotocore
```

### From goaws

```bash
# Before (goaws)
docker run -p 4100:4100 admiralpiett/goaws

# After (Robotocore)
docker run -p 4566:4566 robotocore/robotocore
```

Update your endpoint:

```
# goaws default
http://localhost:4100

# Robotocore
http://localhost:4566
```

Or map the port: `docker run -p 4100:4566 robotocore/robotocore`

---

## SDK / CLI Endpoint Changes

### AWS CLI

```bash
# ElasticMQ
aws sqs list-queues --endpoint-url http://localhost:9324

# goaws
aws sqs list-queues --endpoint-url http://localhost:4100

# Robotocore
aws sqs list-queues --endpoint-url http://localhost:4566
```

### Python (boto3)

```python
sqs = boto3.client("sqs", endpoint_url="http://localhost:4566", region_name="us-east-1")
```

### JavaScript (AWS SDK v3)

```javascript
const client = new SQSClient({
  endpoint: "http://localhost:4566",
  region: "us-east-1",
});
```

### Java (AWS SDK v2)

```java
SqsClient client = SqsClient.builder()
    .endpointOverride(URI.create("http://localhost:4566"))
    .region(Region.US_EAST_1)
    .build();
```

### Go (AWS SDK v2)

```go
cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithEndpointResolverWithOptions(
        aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:4566"}, nil
        }),
    ),
)
client := sqs.NewFromConfig(cfg)
```

---

## Operation Parity

### ElasticMQ vs. Robotocore

All 23 ElasticMQ operations are supported natively (not via Moto fallback):

| Operation                      | ElasticMQ | Robotocore | Notes                        |
|-------------------------------|-----------|------------|------------------------------|
| CreateQueue                   | Yes       | Yes        | Native                       |
| DeleteQueue                   | Yes       | Yes        | Native                       |
| GetQueueUrl                   | Yes       | Yes        | Native                       |
| ListQueues                    | Yes       | Yes        | Native                       |
| SendMessage                   | Yes       | Yes        | Native                       |
| SendMessageBatch              | Yes       | Yes        | Native, validates batch size |
| ReceiveMessage                | Yes       | Yes        | Native, async long polling   |
| DeleteMessage                 | Yes       | Yes        | Native                       |
| DeleteMessageBatch            | Yes       | Yes        | Native                       |
| ChangeMessageVisibility       | Yes       | Yes        | Native                       |
| ChangeMessageVisibilityBatch  | Yes       | Yes        | Native                       |
| PurgeQueue                    | Yes       | Yes        | Native, enforces 60s cooldown|
| GetQueueAttributes            | Yes       | Yes        | Native                       |
| SetQueueAttributes            | Yes       | Yes        | Native                       |
| AddPermission                 | Yes       | Yes        | Native, builds IAM policy    |
| RemovePermission              | Yes       | Yes        | Native                       |
| TagQueue                      | Yes       | Yes        | Native                       |
| UntagQueue                    | Yes       | Yes        | Native                       |
| ListQueueTags                 | Yes       | Yes        | Native                       |
| ListDeadLetterSourceQueues    | Yes       | Yes        | Native                       |
| StartMessageMoveTask          | Yes       | Yes        | Native                       |
| CancelMessageMoveTask         | Yes       | Yes        | Native                       |
| ListMessageMoveTasks          | Yes       | Yes        | Native                       |

Any additional SQS operations not listed above (e.g., future AWS additions) fall back to Moto automatically.

### goaws vs. Robotocore

All 14 goaws SQS operations are supported, plus 9 more:

| Operation                      | goaws | Robotocore | Notes                        |
|-------------------------------|-------|------------|------------------------------|
| CreateQueue                   | Yes   | Yes        | Native                       |
| DeleteQueue                   | Yes   | Yes        | Native                       |
| GetQueueUrl                   | Yes   | Yes        | Native                       |
| ListQueues                    | Yes   | Yes        | Native                       |
| SendMessage                   | Yes   | Yes        | Native                       |
| SendMessageBatch              | Yes   | Yes        | Native                       |
| ReceiveMessage                | Yes   | Yes        | Native                       |
| DeleteMessage                 | Yes   | Yes        | Native                       |
| DeleteMessageBatch            | Yes   | Yes        | Native                       |
| ChangeMessageVisibility       | Yes   | Yes        | Native                       |
| ChangeMessageVisibilityBatch  | Yes   | Yes        | Native                       |
| PurgeQueue                    | Yes   | Yes        | Native                       |
| GetQueueAttributes            | Yes   | Yes        | Full attribute support        |
| ListDeadLetterSourceQueues    | Yes   | Yes        | Native                       |
| SetQueueAttributes            | No    | Yes        | Native                       |
| AddPermission                 | No    | Yes        | Native                       |
| RemovePermission              | No    | Yes        | Native                       |
| TagQueue                      | No    | Yes        | Native                       |
| UntagQueue                    | No    | Yes        | Native                       |
| ListQueueTags                 | No    | Yes        | Native                       |
| StartMessageMoveTask          | No    | Yes        | Native                       |
| CancelMessageMoveTask         | No    | Yes        | Native                       |
| ListMessageMoveTasks          | No    | Yes        | Native                       |

---

## Configuration Migration

### ElasticMQ: elasticmq.conf queues

ElasticMQ pre-defines queues in HOCON config:

```hocon
queues {
  orders {
    defaultVisibilityTimeout = 30 seconds
    delay = 5 seconds
    receiveMessageWait = 10 seconds
    deadLettersQueue {
      name = "orders-dlq"
      maxReceiveCount = 3
    }
    fifo = false
    tags {
      env = "dev"
      team = "platform"
    }
  }
  orders-dlq {}
  notifications.fifo {
    fifo = true
    contentBasedDeduplication = true
  }
}
```

Robotocore equivalent -- create queues via the SQS API at startup. Use a shell script, SDK call, or test fixture:

```bash
#!/bin/bash
ENDPOINT="http://localhost:4566"

# Create the DLQ first
aws sqs create-queue \
  --queue-name orders-dlq \
  --endpoint-url $ENDPOINT

# Create main queue with DLQ redrive policy
DLQ_ARN="arn:aws:sqs:us-east-1:123456789012:orders-dlq"
aws sqs create-queue \
  --queue-name orders \
  --attributes '{
    "VisibilityTimeout": "30",
    "DelaySeconds": "5",
    "ReceiveMessageWaitTimeSeconds": "10",
    "RedrivePolicy": "{\"deadLetterTargetArn\":\"'"$DLQ_ARN"'\",\"maxReceiveCount\":\"3\"}"
  }' \
  --tags '{"env":"dev","team":"platform"}' \
  --endpoint-url $ENDPOINT

# FIFO queue
aws sqs create-queue \
  --queue-name notifications.fifo \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"true"}' \
  --endpoint-url $ENDPOINT
```

### goaws: goaws.yaml queues

goaws pre-defines queues in YAML:

```yaml
Local:
  Host: localhost
  Port: 4100
  Region: us-east-1
  QueueAttributeDefaults:
    VisibilityTimeout: 30
  Queues:
    - Name: orders
      RedrivePolicy: '{"maxReceiveCount": 3, "deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789012:orders-dlq"}'
    - Name: orders-dlq
    - Name: events.fifo
      IsFIFO: true
```

Robotocore equivalent -- same approach as ElasticMQ above. Create queues via API calls:

```bash
ENDPOINT="http://localhost:4566"

aws sqs create-queue --queue-name orders-dlq --endpoint-url $ENDPOINT

aws sqs create-queue --queue-name orders \
  --attributes '{"RedrivePolicy":"{\"maxReceiveCount\":\"3\",\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:123456789012:orders-dlq\"}"}' \
  --endpoint-url $ENDPOINT

aws sqs create-queue --queue-name events.fifo \
  --attributes '{"FifoQueue":"true"}' \
  --endpoint-url $ENDPOINT
```

### Tip: Init script in Docker Compose

```yaml
services:
  robotocore:
    image: robotocore/robotocore
    ports:
      - "4566:4566"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:4566/_robotocore/health"]
      interval: 5s
      retries: 3

  init-queues:
    image: amazon/aws-cli
    depends_on:
      robotocore:
        condition: service_healthy
    environment:
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_DEFAULT_REGION: us-east-1
    entrypoint: /bin/sh -c
    command: |
      "
      aws sqs create-queue --queue-name orders-dlq --endpoint-url http://robotocore:4566
      aws sqs create-queue --queue-name orders --endpoint-url http://robotocore:4566 \
        --attributes '{\"RedrivePolicy\":\"{\\\"maxReceiveCount\\\":\\\"3\\\",\\\"deadLetterTargetArn\\\":\\\"arn:aws:sqs:us-east-1:123456789012:orders-dlq\\\"}\"}'
      "
```

---

## What Robotocore Adds Beyond Each Emulator

### Beyond ElasticMQ

| Capability                    | ElasticMQ          | Robotocore                              |
|------------------------------|--------------------|-----------------------------------------|
| Other AWS services           | SQS only           | 147 services (S3, Lambda, DynamoDB...)  |
| PurgeQueue 60s cooldown      | No                 | Yes -- returns PurgeQueueInProgress     |
| QueueDeletedRecently error   | No                 | Yes -- 60s re-create block like AWS     |
| Message retention expiry     | No                 | Yes -- background scanner removes old msgs |
| Chaos engineering            | No                 | Inject faults, latency, throttling      |
| State snapshots              | Persistence (H2)   | Save/load named snapshots via API       |
| Audit log                    | No                 | Full API call history                   |
| Resource browser             | Web UI (read-only) | Cross-service resource overview          |
| IAM policy enforcement       | No                 | Opt-in via ENFORCE_IAM=1                |
| DLQ redrive (max receive)    | Yes                | Yes                                     |
| FIFO deduplication           | Yes                | Yes                                     |

### Beyond goaws

| Capability                    | goaws              | Robotocore                              |
|------------------------------|--------------------|-----------------------------------------|
| Other AWS services           | SQS + SNS only     | 147 services                            |
| SetQueueAttributes           | Incomplete         | Full support                            |
| Tagging (Tag/Untag/List)     | No                 | Yes                                     |
| Permissions (Add/Remove)     | No                 | Yes                                     |
| Message move tasks           | No                 | Yes (Start/Cancel/List)                 |
| PurgeQueue 60s cooldown      | No                 | Yes                                     |
| QueueDeletedRecently error   | No                 | Yes                                     |
| Message retention expiry     | No                 | Yes                                     |
| Chaos engineering            | No                 | Yes                                     |
| State snapshots              | No                 | Yes                                     |
| Long polling (async)         | Basic              | Non-blocking async via asyncio          |
| FIFO queues                  | Yes                | Yes                                     |
| Dead letter queues           | Yes                | Yes, with automatic redrive             |

---

## Known Differences and Caveats

### ElasticMQ features with no Robotocore equivalent

- **`copyTo` / `moveTo` operations**: ElasticMQ test helpers for copying or moving messages between queues. No equivalent in Robotocore (or AWS). Use `StartMessageMoveTask` for DLQ redrive, or implement send+delete in your test code.
- **Web UI dashboard** (port 9325): ElasticMQ provides a browser-based queue inspector. Robotocore offers `GET /_robotocore/resources` (JSON) and the audit log instead.
- **HOCON config file**: Queues must be created via API calls rather than a config file. See the configuration migration section above.
- **Relaxed/strict limits mode**: ElasticMQ can relax message size and other limits. Robotocore enforces AWS-like limits.
- **H2 message persistence**: ElasticMQ can persist messages to disk. Robotocore is in-memory with snapshot save/load.

### goaws features with no Robotocore equivalent

- **YAML config file**: Queues must be created via API calls. See configuration migration above.
- **Combined SQS+SNS config**: goaws lets you wire SNS subscriptions to SQS in config. In Robotocore, create the SNS subscription via the SNS API (both services are available on the same port).

### Behavioral differences from both

- **Account ID**: Robotocore uses `123456789012` by default. Queue ARNs and URLs include this. If your tests hard-code a different account ID, update them or set the account via the `Authorization` header.
- **Region**: Defaults to `us-east-1`. Robotocore supports multi-region (each region has isolated state).
- **Queue URL format**: `http://localhost:4566/123456789012/queue-name` (matches AWS format).
- **Both JSON and query protocols**: Robotocore handles modern boto3 (JSON protocol with `x-amz-json-1.0`) and legacy query protocol. No configuration needed.

### State management

Robotocore SQS state is in-memory by default. If no snapshot or persistence settings are enabled, container restart resets queues and messages. To preserve SQS state across restarts or between test phases:

```bash
# Save state
curl -X POST http://localhost:4566/_robotocore/state/save \
  -d '{"name": "my-snapshot"}'

# Load state
curl -X POST http://localhost:4566/_robotocore/state/load \
  -d '{"name": "my-snapshot"}'

# Save only SQS state
curl -X POST http://localhost:4566/_robotocore/state/save \
  -d '{"name": "sqs-only", "services": ["sqs"]}'
```

You can also configure persistent startup/shutdown save behavior with `ROBOTOCORE_STATE_DIR` and persistence env vars. Native SQS queues, messages, receipts, and FIFO ordering state participate in that existing state-manager flow.

---

## Docker Run Examples

### Minimal

```bash
docker run -p 4566:4566 robotocore/robotocore
```

### With environment variables

```bash
docker run -p 4566:4566 \
  -e ROBOTOCORE_PORT=4566 \
  -e AUDIT_LOG_SIZE=1000 \
  robotocore/robotocore
```

### Port-compatible with ElasticMQ

```bash
docker run -p 9324:4566 robotocore/robotocore
```

### Port-compatible with goaws

```bash
docker run -p 4100:4566 robotocore/robotocore
```

### Docker Compose (replacing ElasticMQ)

```yaml
services:
  # Replace this:
  # elasticmq:
  #   image: softwaremill/elasticmq-native
  #   ports:
  #     - "9324:9324"

  # With this:
  aws:
    image: robotocore/robotocore
    ports:
      - "9324:4566"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:4566/_robotocore/health"]
      interval: 5s
      retries: 3
```

### Docker Compose (replacing goaws)

```yaml
services:
  # Replace this:
  # goaws:
  #   image: admiralpiett/goaws
  #   ports:
  #     - "4100:4100"

  # With this:
  aws:
    image: robotocore/robotocore
    ports:
      - "4100:4566"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:4566/_robotocore/health"]
      interval: 5s
      retries: 3
```
