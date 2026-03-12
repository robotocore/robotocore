# Migrating from goaws (SNS) to Robotocore

Drop-in migration guide for teams moving from [goaws](https://github.com/Admiral-Piett/goaws) SNS to Robotocore.

## Quick start

```bash
# Replace this:
docker run -p 4100:4100 admiralpiett/goaws

# With this:
docker run -p 4566:4566 robotocore/robotocore
```

Then update your endpoint:

```bash
# Before (goaws)
aws --endpoint-url=http://localhost:4100 sns create-topic --name my-topic

# After (Robotocore)
aws --endpoint-url=http://localhost:4566 sns create-topic --name my-topic
```

Everything else (credentials, region, etc.) stays the same. Robotocore accepts any credentials.

## Operation parity table

| Operation                  | goaws | Robotocore | Notes                                      |
|----------------------------|:-----:|:----------:|---------------------------------------------|
| CreateTopic                |  Yes  |    Yes     |                                             |
| DeleteTopic                |  Yes  |    Yes     |                                             |
| ListTopics                 |  Yes  |    Yes     |                                             |
| Subscribe                  |  Yes  |    Yes     | Robotocore uses standard ARNs, not queue URLs |
| Unsubscribe                |  Yes  |    Yes     |                                             |
| ListSubscriptions          |  Yes  |    Yes     |                                             |
| ListSubscriptionsByTopic   |  Yes  |    Yes     |                                             |
| Publish                    |  Yes  |    Yes     | Message attributes propagated to SQS        |
| PublishBatch               |  No   |    Yes     |                                             |
| SetTopicAttributes         |  No   |    Yes     |                                             |
| GetTopicAttributes         |  No   |    Yes     |                                             |
| SetSubscriptionAttributes  |  No   |    Yes     |                                             |
| GetSubscriptionAttributes  |  No   |    Yes     |                                             |
| ConfirmSubscription        |  No   |    Yes     |                                             |
| TagResource                |  No   |    Yes     |                                             |
| UntagResource              |  No   |    Yes     |                                             |
| ListTagsForResource        |  No   |    Yes     |                                             |
| CreatePlatformApplication  |  No   |    Yes     |                                             |
| CreatePlatformEndpoint     |  No   |    Yes     |                                             |
| CheckIfPhoneNumberIsOptedOut | No  |    Yes     |                                             |

## Configuration mapping

goaws uses a YAML config file to pre-create topics and subscriptions. Robotocore uses the standard AWS API instead.

### goaws.yaml (before)

```yaml
Local:
  Host: localhost
  Port: 4100
  Region: us-east-1
  AccountId: "100010001000"

Topics:
  - Name: order-events
  - Name: user-events

Subscriptions:
  - QueueName: order-processor
    TopicName: order-events
    Raw: true
```

### Robotocore equivalent (after)

Create an init script that runs on startup:

```bash
#!/bin/bash
ENDPOINT=http://localhost:4566

# Create topics
aws --endpoint-url=$ENDPOINT sns create-topic --name order-events
aws --endpoint-url=$ENDPOINT sns create-topic --name user-events

# Create the target queue
aws --endpoint-url=$ENDPOINT sqs create-queue --queue-name order-processor

# Subscribe using ARN (not queue URL)
aws --endpoint-url=$ENDPOINT sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:123456789012:order-events \
  --protocol sqs \
  --notification-endpoint arn:aws:sqs:us-east-1:123456789012:order-processor \
  --attributes '{"RawMessageDelivery":"true"}'
```

Or use a docker-compose setup:

```yaml
services:
  robotocore:
    image: robotocore/robotocore
    ports:
      - "4566:4566"
  init:
    image: amazon/aws-cli
    depends_on:
      - robotocore
    entrypoint: /bin/sh
    command: -c "/init.sh"
    volumes:
      - ./init.sh:/init.sh
    environment:
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_DEFAULT_REGION: us-east-1
```

Alternatively, save state after setup and reload it:

```bash
# After creating your resources:
curl -X POST http://localhost:4566/_robotocore/state/save -d '{"name":"sns-setup"}'

# On next startup:
curl -X POST http://localhost:4566/_robotocore/state/load -d '{"name":"sns-setup"}'
```

## SDK / CLI endpoint changes

### AWS CLI

```bash
# Global env var (applies to all services)
export AWS_ENDPOINT_URL=http://localhost:4566

# Or per-command
aws --endpoint-url=http://localhost:4566 sns publish ...
```

### Python (boto3)

```python
# Before (goaws)
client = boto3.client("sns", endpoint_url="http://localhost:4100")

# After (Robotocore)
client = boto3.client("sns", endpoint_url="http://localhost:4566")
```

### JavaScript (AWS SDK v3)

```javascript
// Before
const client = new SNSClient({ endpoint: "http://localhost:4100" });

// After
const client = new SNSClient({ endpoint: "http://localhost:4566" });
```

### Go (aws-sdk-go-v2)

```go
// Before
cfg.EndpointResolverWithOptions = aws.EndpointResolverWithOptionsFunc(
    func(service, region string, options ...interface{}) (aws.Endpoint, error) {
        return aws.Endpoint{URL: "http://localhost:4100"}, nil
    })

// After — change port to 4566
cfg.EndpointResolverWithOptions = aws.EndpointResolverWithOptionsFunc(
    func(service, region string, options ...interface{}) (aws.Endpoint, error) {
        return aws.Endpoint{URL: "http://localhost:4566"}, nil
    })
```

### AWS_ENDPOINT_URL (SDK-native, no code changes)

Supported by AWS SDK v2+ (Python, JS, Go, Java, .NET):

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
# All SDK calls now route to Robotocore — no code changes needed
```

## What's new in Robotocore SNS

Features not available in goaws:

- **Message filtering**: `FilterPolicy` on subscriptions to route messages by attributes
- **FIFO topics**: `CreateTopic` with `FifoTopic=true`, deduplication, ordering
- **Batch publish**: `PublishBatch` for up to 10 messages per call
- **HTTP/HTTPS subscriptions**: Subscribe any HTTP endpoint, with confirmation flow
- **Lambda targets**: `Protocol=lambda` delivers to Robotocore Lambda functions
- **Email protocol**: Subscribe email addresses (messages captured in-memory)
- **Platform applications**: Mobile push notification modeling (APNS, GCM, etc.)
- **Message attributes**: Fully propagated to SQS subscribers
- **Topic/subscription attributes**: Get/Set for delivery policies, filtering, raw delivery
- **Tagging**: Full tag CRUD on topics
- **146 other AWS services**: SQS, Lambda, DynamoDB, S3, etc. on the same endpoint

## Known differences and caveats

### goaws quirks that do NOT carry over

1. **Queue URL subscriptions**: goaws requires SQS queue URLs in `Subscribe`. Robotocore uses standard SQS ARNs (matching real AWS behavior). If your code passes queue URLs, change them to ARNs.

2. **Account ID**: goaws defaults to `100010001000`. Robotocore defaults to `123456789012`. Update ARNs in your test fixtures if they're hardcoded.

3. **Port**: goaws runs on `4100`. Robotocore runs on `4566`. Update endpoint URLs.

### Robotocore-specific notes

- **In-memory state**: All topics/subscriptions are lost on container restart unless you use snapshots (`/_robotocore/state/save`).
- **No goaws.yaml support**: Pre-creation of resources must use the AWS API (see Configuration mapping above).
- **Default region**: `us-east-1` (same as goaws).
- **Credentials**: Any value accepted; no validation.

## Docker run examples

```bash
# Basic
docker run -p 4566:4566 robotocore/robotocore

# With debug logging
docker run -p 4566:4566 -e DEBUG=1 robotocore/robotocore

# With persistent state directory
docker run -p 4566:4566 -v robotocore-data:/data robotocore/robotocore

# Replacing goaws in docker-compose
# Change image and port, remove goaws.yaml volume, add init script
```
