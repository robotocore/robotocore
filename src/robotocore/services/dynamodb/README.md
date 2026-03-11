# Migrating to Robotocore DynamoDB

Migration guide for teams moving from **dynalite** or **DynamoDB Local** to Robotocore.

---

## 1. Quick Start

### From dynalite

```bash
# Before (dynalite)
docker run -p 4567:4567 kpavlov/dynalite

# After (Robotocore)
docker run -p 4566:4566 robotocore/robotocore
```

Change your endpoint from `http://localhost:4567` to `http://localhost:4566`.

### From DynamoDB Local

```bash
# Before (DynamoDB Local)
docker run -p 8000:8000 amazon/dynamodb-local

# After (Robotocore)
docker run -p 4566:4566 robotocore/robotocore
```

Change your endpoint from `http://localhost:8000` to `http://localhost:4566`.

If you want to keep using port 8000 during migration:

```bash
docker run -p 8000:4566 robotocore/robotocore
```

---

## 2. Operation Parity

### dynalite (17 operations) -> Robotocore

All 17 dynalite operations are supported.

| Operation              | dynalite | Robotocore |
|------------------------|----------|------------|
| BatchGetItem           | Yes      | Yes        |
| BatchWriteItem         | Yes      | Yes        |
| CreateTable            | Yes      | Yes        |
| DeleteItem             | Yes      | Yes        |
| DeleteTable            | Yes      | Yes        |
| DescribeTable          | Yes      | Yes        |
| DescribeTimeToLive     | Yes      | Yes        |
| GetItem                | Yes      | Yes        |
| ListTables             | Yes      | Yes        |
| ListTagsOfResource     | Yes      | Yes        |
| PutItem                | Yes      | Yes        |
| Query                  | Yes      | Yes        |
| Scan                   | Yes      | Yes        |
| TagResource            | Yes      | Yes        |
| UntagResource          | Yes      | Yes        |
| UpdateItem             | Yes      | Yes        |
| UpdateTable            | Yes      | Yes        |

### DynamoDB Local -> Robotocore

| Operation                          | DynamoDB Local | Robotocore |
|------------------------------------|----------------|------------|
| CRUD (Get/Put/Update/Delete Item)  | Yes            | Yes        |
| Batch operations                   | Yes            | Yes        |
| Table management (Create/Delete/Update/Describe) | Yes | Yes   |
| Query and Scan                     | Yes            | Yes        |
| TransactGetItems                   | Yes            | Yes        |
| TransactWriteItems                 | Yes            | Yes        |
| DynamoDB Streams                   | Yes            | Yes (via dynamodbstreams service) |
| Global Secondary Indexes           | Yes            | Yes        |
| Local Secondary Indexes            | Yes            | Yes        |
| PartiQL (ExecuteStatement, etc.)   | Yes            | Yes        |
| TTL (DescribeTimeToLive, UpdateTimeToLive) | Yes    | Yes        |
| Tagging                            | No             | Yes        |
| Global Tables                      | No             | Yes (CreateGlobalTable, UpdateGlobalTable, replication) |
| DescribeTableReplicaAutoScaling    | No             | Yes (stub) |
| DescribeLimits                     | No             | Yes        |
| DescribeContinuousBackups          | No             | Yes        |
| Backups (Create/Delete/Describe)   | No             | Yes        |
| Encryption (DescribeKinesisDest.)  | No             | No         |

### Operations NOT supported by dynalite that Robotocore adds

- TransactGetItems, TransactWriteItems
- DynamoDB Streams (GetRecords, GetShardIterator, DescribeStream, ListStreams)
- Global Tables (Create, Update, Delete, Describe, List)
- PartiQL (ExecuteStatement, BatchExecuteStatement, ExecuteTransaction)
- Backups (CreateBackup, DeleteBackup, DescribeBackup, ListBackups, RestoreTableFromBackup)
- DescribeLimits, DescribeContinuousBackups, DescribeEndpoints
- Table auto-scaling descriptions

---

## 3. Configuration Mapping

### dynalite flags -> Robotocore

| dynalite flag       | Purpose                  | Robotocore equivalent                          |
|---------------------|--------------------------|-------------------------------------------------|
| `--port 4567`       | Listen port              | `-p <host_port>:4566` (Docker port mapping)     |
| `--path ./data`     | LevelDB persistence      | State snapshots (see below)                     |
| `--createTableMs`   | Simulate table creation delay | Chaos API: `POST /_robotocore/chaos/rules`  |
| `--deleteTableMs`   | Simulate table deletion delay | Chaos API: `POST /_robotocore/chaos/rules`  |
| `--updateTableMs`   | Simulate table update delay   | Chaos API: `POST /_robotocore/chaos/rules`  |
| `--maxItemSizeKb`   | Max item size            | Not configurable (uses DynamoDB default: 400KB) |
| `--ssl`             | Enable TLS               | Use a reverse proxy (nginx, caddy) in front     |

### DynamoDB Local flags -> Robotocore

| DynamoDB Local flag               | Purpose                 | Robotocore equivalent                          |
|-----------------------------------|-------------------------|-------------------------------------------------|
| `-port 8000`                      | Listen port             | `-p <host_port>:4566` (Docker port mapping)     |
| `-inMemory`                       | No disk persistence     | Default behavior (always in-memory)             |
| `-sharedDb`                       | Single DB for all creds | Default behavior (account isolation available)  |
| `-dbPath ./data`                  | SQLite persistence path | State snapshots: `POST /_robotocore/state/save` |
| `-cors *`                         | CORS origins            | Built-in CORS support                           |
| `-optimizeDbBeforeStartup`        | Compact before start    | Not applicable (in-memory)                      |

### Persistence via State Snapshots

Robotocore does not use disk-based persistence by default. Instead, use the snapshot API:

```bash
# Save state (all services, or just DynamoDB)
curl -X POST http://localhost:4566/_robotocore/state/save \
  -d '{"name": "my-snapshot"}'

curl -X POST http://localhost:4566/_robotocore/state/save \
  -d '{"name": "dynamo-only", "services": ["dynamodb"]}'

# Restore state
curl -X POST http://localhost:4566/_robotocore/state/load \
  -d '{"name": "my-snapshot"}'
```

### Simulating Delays (Chaos API)

Replace dynalite's `--createTableMs` / `--deleteTableMs` flags:

```bash
# Add 500ms delay to CreateTable
curl -X POST http://localhost:4566/_robotocore/chaos/rules -d '{
  "service": "dynamodb",
  "operation": "CreateTable",
  "effect": {"type": "latency", "ms": 500}
}'

# Inject ThrottlingException on 20% of PutItem calls
curl -X POST http://localhost:4566/_robotocore/chaos/rules -d '{
  "service": "dynamodb",
  "operation": "PutItem",
  "effect": {"type": "error", "code": "ThrottlingException", "rate": 0.2}
}'
```

---

## 4. SDK and CLI Endpoint Configuration

### AWS CLI

```bash
# Per-command
aws dynamodb list-tables --endpoint-url http://localhost:4566

# Environment variable (applies to all AWS services)
export AWS_ENDPOINT_URL=http://localhost:4566
aws dynamodb list-tables

# AWS CLI config (~/.aws/config)
[profile local]
endpoint_url = http://localhost:4566
```

### Python (boto3)

```python
import boto3

# Before (DynamoDB Local)
client = boto3.client("dynamodb", endpoint_url="http://localhost:8000")

# After (Robotocore)
client = boto3.client("dynamodb", endpoint_url="http://localhost:4566")

# Or use the environment variable (no code changes needed)
# export AWS_ENDPOINT_URL=http://localhost:4566
client = boto3.client("dynamodb")  # picks up AWS_ENDPOINT_URL automatically
```

### JavaScript / TypeScript (AWS SDK v3)

```typescript
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

// Before (dynalite)
const client = new DynamoDBClient({ endpoint: "http://localhost:4567" });

// After (Robotocore)
const client = new DynamoDBClient({ endpoint: "http://localhost:4566" });

// Or set AWS_ENDPOINT_URL=http://localhost:4566 — SDK v3 reads it automatically
```

### Go (AWS SDK v2)

```go
import (
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Option 1: explicit endpoint
cfg, _ := config.LoadDefaultConfig(ctx)
client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
    o.BaseEndpoint = aws.String("http://localhost:4566")
})

// Option 2: AWS_ENDPOINT_URL=http://localhost:4566 (no code changes)
```

### Java (AWS SDK v2)

```java
DynamoDbClient client = DynamoDbClient.builder()
    .endpointOverride(URI.create("http://localhost:4566"))
    .region(Region.US_EAST_1)
    .build();
```

### Credentials

Robotocore accepts any credentials. For local development:

```bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
```

---

## 5. What Robotocore Adds Beyond Each Emulator

### Beyond dynalite

- **Full DynamoDB API**: transactions, streams, PartiQL, global tables, backups, TTL
- **146 other AWS services**: S3, SQS, Lambda, IAM, etc. -- all on the same endpoint
- **DynamoDB Streams with hooks**: mutations fire stream events consumed by the dynamodbstreams service
- **Global Table replication**: writes replicate across regions within the emulator
- **Chaos engineering**: inject latency, errors, throttling per-operation
- **State snapshots**: save/load full state or per-service state
- **Account/region isolation**: test multi-account and multi-region workflows
- **Audit log**: `GET /_robotocore/audit` to inspect recent API calls
- **Resource browser**: `GET /_robotocore/resources` for a cross-service resource overview

### Beyond DynamoDB Local

- **No Java dependency**: Python-based, smaller container image
- **146 other AWS services**: test cross-service workflows (e.g., DynamoDB Streams triggering Lambda)
- **Tagging support**: TagResource, UntagResource, ListTagsOfResource all work
- **Global Tables**: CreateGlobalTable, UpdateGlobalTable with cross-region replication
- **Chaos engineering**: fault injection that DynamoDB Local cannot simulate
- **State snapshots**: replace `-dbPath` with API-driven save/load
- **Audit and observability**: structured logs, API call history, diagnostics headers

---

## 6. Known Differences and Caveats

### vs. real AWS DynamoDB

- **No capacity throttling**: reads and writes are not throttled by provisioned capacity
- **Table creation is instant**: tables are immediately ACTIVE (no CREATING state)
- **No parallel scan segments**: `TotalSegments` is accepted but not parallelized server-side
- **No encryption at rest**: encryption settings are accepted but not enforced
- **Stream shard behavior**: stream shards are simplified compared to real DynamoDB Streams
- **Item size limit**: enforced at 400KB (matching AWS), but edge cases in size calculation may differ

### vs. dynalite

- **Port change**: 4567 -> 4566 (or remap with `-p 4567:4566`)
- **No LevelDB persistence**: use state snapshots instead of `--path`
- **Response format**: Robotocore uses Moto's response serialization, which matches AWS more closely than dynalite in most cases

### vs. DynamoDB Local

- **Port change**: 8000 -> 4566 (or remap with `-p 8000:4566`)
- **No `-sharedDb` flag**: Robotocore isolates by account ID and region by default. All requests with the same credentials share state, which is equivalent to `-sharedDb` for most setups.
- **No SQLite persistence**: use state snapshots for persistence between restarts

---

## 7. Docker Run Examples

### Basic (drop-in replacement)

```bash
docker run -d --name robotocore -p 4566:4566 robotocore/robotocore
```

### Keep existing dynalite port

```bash
docker run -d --name robotocore -p 4567:4566 robotocore/robotocore
```

### Keep existing DynamoDB Local port

```bash
docker run -d --name robotocore -p 8000:4566 robotocore/robotocore
```

### With docker-compose

```yaml
services:
  robotocore:
    image: robotocore/robotocore
    ports:
      - "4566:4566"
    environment:
      - ROBOTOCORE_HOST=0.0.0.0
      - ROBOTOCORE_PORT=4566
```

### Health check

```bash
curl http://localhost:4566/_robotocore/health
```

### Verify DynamoDB is working

```bash
aws dynamodb list-tables --endpoint-url http://localhost:4566
aws dynamodb create-table \
  --table-name test \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --endpoint-url http://localhost:4566
```
