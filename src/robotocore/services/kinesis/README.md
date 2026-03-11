# Migrating from kinesalite to Robotocore

Robotocore's Kinesis provider is a native implementation with 28 natively-handled operations (plus Moto fallback for others like DescribeLimits). It is a superset of kinesalite's 17 operations. All existing kinesalite workflows should work without code changes beyond updating the endpoint URL and port.

## Quick start

```bash
# Before (kinesalite)
docker run -p 4567:4567 dlsteuer/kinesalite

# After (Robotocore)
docker run -p 4566:4566 robotocore/robotocore
```

Update your SDK/CLI endpoint from `http://localhost:4567` to `http://localhost:4566`. That's it.

## Operation parity

Every kinesalite operation is supported. The table below maps all 17:

| Operation | kinesalite | Robotocore | Notes |
|---|---|---|---|
| CreateStream | Yes | Yes | |
| DeleteStream | Yes | Yes | Also cleans up resource policies |
| DescribeStream | Yes | Yes | Includes pagination (Limit, ExclusiveStartShardId) |
| DescribeStreamSummary | Yes | Yes | Includes ConsumerCount |
| ListStreams | Yes | Yes | |
| ListShards | Yes | Yes | NextToken-based pagination |
| PutRecord | Yes | Yes | ExplicitHashKey supported |
| PutRecords | Yes | Yes | |
| GetShardIterator | Yes | Yes | All 5 iterator types (TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER, AT_TIMESTAMP) |
| GetRecords | Yes | Yes | |
| AddTagsToStream | Yes | Yes | |
| RemoveTagsFromStream | Yes | Yes | |
| ListTagsForStream | Yes | Yes | Pagination via ExclusiveStartTagKey |
| IncreaseStreamRetentionPeriod | Yes | Yes | |
| DecreaseStreamRetentionPeriod | Yes | Yes | Enforces 24-hour minimum |
| SplitShard | Yes | Yes | |
| MergeShards | Yes | Yes | |

## Configuration mapping

| kinesalite flag | Robotocore equivalent | Details |
|---|---|---|
| `--port 4567` | `-p 4566:4566` | Robotocore always listens on 4566 inside the container. Map to any host port you want. |
| `--path /data` | State snapshots API | See [Persistence](#persistence) below. |
| `--createStreamMs 500` | Chaos engineering API | `POST /_robotocore/chaos/rules` with latency injection. See [Simulating delays](#simulating-delays). |
| `--deleteStreamMs 500` | Chaos engineering API | Same mechanism, target DeleteStream. |
| `--updateStreamMs 500` | Chaos engineering API | Same mechanism, target the relevant operation. |
| `--shardLimit 100` | Not applicable | No artificial shard limit. Streams accept any shard count. |

### Persistence

kinesalite uses LevelDB via `--path`. Robotocore uses in-memory storage with snapshot save/load:

```bash
# Save state (all services)
curl -X POST http://localhost:4566/_robotocore/state/save \
  -d '{"name": "my-snapshot"}'

# Save state (Kinesis only)
curl -X POST http://localhost:4566/_robotocore/state/save \
  -d '{"name": "my-snapshot", "services": ["kinesis"]}'

# Restore state
curl -X POST http://localhost:4566/_robotocore/state/load \
  -d '{"name": "my-snapshot"}'
```

### Simulating delays

kinesalite's `--createStreamMs` etc. flags simulate AWS propagation delays. Robotocore's chaos engineering API provides the same capability with more flexibility:

```bash
# Add 500ms latency to CreateStream
curl -X POST http://localhost:4566/_robotocore/chaos/rules \
  -d '{
    "service": "kinesis",
    "operation": "CreateStream",
    "effect": {"type": "latency", "ms": 500}
  }'
```

You can also inject errors (e.g., ThrottlingException) and target any operation, not just create/delete/update.

## SDK and CLI endpoint configuration

### AWS CLI

```bash
# Before
aws kinesis list-streams --endpoint-url http://localhost:4567

# After
aws kinesis list-streams --endpoint-url http://localhost:4566
```

### boto3 (Python)

```python
# Before
client = boto3.client("kinesis", endpoint_url="http://localhost:4567")

# After
client = boto3.client("kinesis", endpoint_url="http://localhost:4566")
```

### AWS SDK for JavaScript

```javascript
// Before
const client = new KinesisClient({
  endpoint: "http://localhost:4567",
});

// After
const client = new KinesisClient({
  endpoint: "http://localhost:4566",
});
```

### AWS SDK for Go

```go
// Before
cfg.EndpointResolver = aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
    return aws.Endpoint{URL: "http://localhost:4567"}, nil
})

// After
cfg.EndpointResolver = aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
    return aws.Endpoint{URL: "http://localhost:4566"}, nil
})
```

### Environment variable (works with any SDK)

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
```

## What's new (beyond kinesalite)

Robotocore supports 14 operations that kinesalite does not:

| Operation | What it does |
|---|---|
| UpdateShardCount | Resize a stream's shard count (UNIFORM_SCALING) |
| StartStreamEncryption | Enable KMS encryption on a stream |
| StopStreamEncryption | Disable encryption on a stream |
| RegisterStreamConsumer | Create an Enhanced Fan-Out consumer |
| DescribeStreamConsumer | Describe a registered consumer |
| ListStreamConsumers | List all consumers on a stream |
| DeregisterStreamConsumer | Remove a consumer |
| EnableEnhancedMonitoring | Enable shard-level CloudWatch metrics |
| DisableEnhancedMonitoring | Disable shard-level metrics |
| PutResourcePolicy | Attach a resource policy to a stream or consumer |
| GetResourcePolicy | Retrieve a resource policy |
| DeleteResourcePolicy | Remove a resource policy |
| DescribeLimits | Account-level shard/throughput limits (Moto fallback) |
| SubscribeToShard | Enhanced Fan-Out push-based reads (Moto fallback) |

Additionally, Robotocore runs 146 other AWS services on the same endpoint. If your tests also use DynamoDB, S3, SQS, Lambda, etc., you no longer need separate emulators for each.

## Known differences and caveats

1. **Port**: Robotocore uses 4566 (not 4567). If you have 4567 hardcoded, update it.
2. **Streams are immediately ACTIVE**: Robotocore does not simulate the CREATING state by default. Streams transition to ACTIVE instantly. Use chaos rules to add delays if your code depends on this.
3. **No LevelDB persistence**: State is in-memory. Use the snapshot API for save/restore between runs.
4. **Shard iterator expiry**: Robotocore does not expire shard iterators after 5 minutes. They remain valid for the lifetime of the stream.
5. **Multi-service container**: Robotocore runs all 147 services in one process. Memory usage is higher than kinesalite alone, but you eliminate the need for separate emulators.
6. **CBOR protocol**: kinesalite supports CBOR encoding. Robotocore uses JSON protocol (`application/x-amz-json-1.1`) only. All modern AWS SDKs default to JSON, so this is unlikely to affect you unless you explicitly enabled CBOR.
7. **Account/region isolation**: Robotocore isolates state per account ID and region. kinesalite has a single global namespace. If your tests use different account IDs or regions, streams are not shared between them.

## Docker run examples

```bash
# Basic (matches kinesalite's default behavior)
docker run -p 4566:4566 robotocore/robotocore

# Custom host port (if you need 4567 for backward compatibility)
docker run -p 4567:4566 robotocore/robotocore

# With debug logging
docker run -p 4566:4566 -e ROBOTOCORE_LOG_LEVEL=DEBUG robotocore/robotocore

# With audit logging (track all API calls)
docker run -p 4566:4566 -e AUDIT_LOG_SIZE=1000 robotocore/robotocore
```
