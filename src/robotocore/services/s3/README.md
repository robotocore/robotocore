# Migrating to Robotocore S3

Drop-in S3 emulator replacement for **s3rver** and **Adobe S3Mock**. One port, full API, zero config.

---

## Quick Start

### From s3rver

```bash
# Before (s3rver)
docker run -p 4569:4569 jbergknoff/s3rver --directory /tmp/s3

# After (Robotocore)
docker run -p 4566:4566 robotocore/robotocore
```

Update your endpoint:

```bash
# Before
aws --endpoint-url http://localhost:4569 s3 ls

# After
aws --endpoint-url http://localhost:4566 s3 ls
```

### From Adobe S3Mock

```bash
# Before (S3Mock)
docker run -p 9090:9090 -e initialBuckets=my-bucket adobe/s3mock

# After (Robotocore)
docker run -p 4566:4566 robotocore/robotocore
```

Update your endpoint:

```bash
# Before
aws --endpoint-url http://localhost:9090 s3 ls

# After
aws --endpoint-url http://localhost:4566 s3 ls
```

> Pre-creating buckets: Robotocore does not have an `initialBuckets` env var.
> Use a startup script or create buckets in your test setup.

---

## Operation Parity: s3rver

| Operation | s3rver | Robotocore | Notes |
|---|---|---|---|
| CreateBucket | Yes | Yes | |
| DeleteBucket | Yes | Yes | |
| ListBuckets | Yes | Yes | |
| PutObject | Yes | Yes | |
| GetObject | Yes | Yes | Range requests supported |
| HeadObject | Yes | Yes | |
| DeleteObject | Yes | Yes | |
| CopyObject | Yes | Yes | |
| PostObject | Yes | Yes | HTML form uploads |
| ListObjects (v1) | Yes | Yes | |
| ListObjectsV2 | Yes | Yes | |
| DeleteObjects (batch) | No | Yes | |
| Multipart Upload | No | Yes | Create, Upload, Complete, Abort, ListParts |
| Versioning | No | Yes | Full: enable, suspend, list versions, delete markers |
| ACLs | No | Yes | Get/Put for buckets and objects |
| Bucket Policies | No | Yes | |
| Lifecycle Rules | No | Yes | |
| Server-Side Encryption | No | Yes | SSE-S3, SSE-KMS |
| CORS | Yes | Yes | |
| Static Website Hosting | Yes | Yes | |
| Event Notifications | Programmatic only | Yes | SNS, SQS, Lambda targets |
| Presigned URLs | No | Yes | GET and PUT |
| Object Tagging | No | Yes | |
| S3 Select | No | Yes | |
| Replication Config | No | Yes | |

**Summary**: Every s3rver operation is supported. Robotocore adds multipart uploads, versioning, ACLs, policies, encryption, lifecycle, tagging, S3 Select, and replication.

---

## Operation Parity: Adobe S3Mock

| Operation | S3Mock | Robotocore | Notes |
|---|---|---|---|
| CreateBucket | Yes | Yes | |
| DeleteBucket | Yes | Yes | |
| HeadBucket | Yes | Yes | |
| ListBuckets | Yes | Yes | |
| PutObject | Yes | Yes | |
| GetObject | Yes | Yes | Range requests in both |
| HeadObject | Yes | Yes | |
| DeleteObject | Yes | Yes | |
| DeleteObjects (batch) | Yes | Yes | |
| CopyObject | Yes | Yes | |
| ListObjects (v1) | Yes | Yes | |
| ListObjectsV2 | Yes | Yes | |
| CreateMultipartUpload | Yes | Yes | |
| UploadPart | Yes | Yes | |
| CompleteMultipartUpload | Yes | Yes | |
| AbortMultipartUpload | Yes | Yes | |
| ListParts | Yes | Yes | |
| Versioning | Basic | Yes | Full lifecycle including delete markers |
| Conditional Requests | Yes | Yes | If-Match, If-None-Match, etc. |
| ACLs | No | Yes | |
| Bucket Policies | No | Yes | |
| Lifecycle Rules | No | Yes | |
| Server-Side Encryption | Limited | Yes | SSE-S3, SSE-KMS |
| CORS | No | Yes | |
| Static Website Hosting | No | Yes | |
| Event Notifications | No | Yes | SNS, SQS, Lambda targets |
| Presigned URLs | No | Yes | |
| Object Tagging | No | Yes | |
| S3 Select | No | Yes | |
| Replication Config | No | Yes | |

**Summary**: Every S3Mock operation is supported. Robotocore adds ACLs, policies, lifecycle, CORS, website hosting, notifications, presigned URLs, tagging, S3 Select, and replication.

---

## Configuration Mapping

### s3rver

| s3rver Flag | Robotocore Equivalent | Notes |
|---|---|---|
| `--port 4569` | `-p 4566:4566` | Fixed internal port 4566 |
| `--address 0.0.0.0` | Binds 0.0.0.0 by default | |
| `--directory /data` | No equivalent | In-memory storage; use snapshots for persistence |
| `--cors` | Built-in | CORS configured per-bucket via S3 API |
| `--website` | Built-in | Website hosting configured per-bucket via S3 API |
| `accessKeyId: S3RVER` | Any valid key format | No credential validation by default |
| `secretAccessKey: S3RVER` | Any valid secret | No credential validation by default |

### Adobe S3Mock

| S3Mock Config | Robotocore Equivalent | Notes |
|---|---|---|
| `initialBuckets=a,b,c` | No equivalent | Create buckets via API in test setup |
| `root=/data` | No equivalent | In-memory storage; use snapshots for persistence |
| `retainFilesOnExit=true` | Snapshot save/load | `POST /_robotocore/state/save` before shutdown |
| Port 9090 (HTTP) | Port 4566 | |
| Port 9191 (HTTPS) | Not applicable | HTTP only (use a TLS proxy if needed) |
| Testcontainers | `robotocore/robotocore` image | Works with any Testcontainers setup |

---

## SDK / CLI Endpoint Changes

### AWS CLI

```bash
# Option 1: per-command
aws --endpoint-url http://localhost:4566 s3api list-buckets

# Option 2: env var
export AWS_ENDPOINT_URL=http://localhost:4566

# Option 3: profile (~/.aws/config)
[profile local]
endpoint_url = http://localhost:4566
```

### Python (boto3)

```python
import boto3

# Before (s3rver)
s3 = boto3.client("s3", endpoint_url="http://localhost:4569")

# Before (S3Mock)
s3 = boto3.client("s3", endpoint_url="http://localhost:9090")

# After (Robotocore)
s3 = boto3.client("s3", endpoint_url="http://localhost:4566")
```

### JavaScript (AWS SDK v3)

```javascript
import { S3Client } from "@aws-sdk/client-s3";

const s3 = new S3Client({
  endpoint: "http://localhost:4566",
  region: "us-east-1",
  forcePathStyle: true, // recommended for local emulators
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
});
```

### Java (AWS SDK v2)

```java
S3Client s3 = S3Client.builder()
    .endpointOverride(URI.create("http://localhost:4566"))
    .region(Region.US_EAST_1)
    .forcePathStyle(true)
    .credentialsProvider(StaticCredentialsProvider.create(
        AwsBasicCredentials.create("test", "test")))
    .build();
```

### Go (AWS SDK v2)

```go
cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
)
s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
    o.BaseEndpoint = aws.String("http://localhost:4566")
    o.UsePathStyle = true
})
```

---

## What You Gain Beyond s3rver / S3Mock

- **146 other AWS services** on the same endpoint (SQS, DynamoDB, Lambda, etc.)
- **Cross-service integration**: S3 event notifications trigger real Lambda executions, deliver to real SQS queues
- **State snapshots**: save and restore state without restarting (`POST /_robotocore/state/save`)
- **Chaos engineering**: inject S3 errors on demand (`POST /_robotocore/chaos/rules`)
- **Audit log**: see every API call (`GET /_robotocore/audit`)
- **Resource browser**: inspect all S3 buckets/objects across accounts (`GET /_robotocore/resources`)
- **Virtual-hosted-style access**: `http://my-bucket.s3.localhost:4566/key` in addition to path-style
- **No filesystem dependency**: works in ephemeral CI environments with no writable volume

---

## Known Differences and Caveats

1. **No filesystem persistence**: s3rver writes to `--directory`, S3Mock writes to `root`. Robotocore stores everything in memory. Use snapshot save/load for persistence across restarts:
   ```bash
   # Save before shutdown
   curl -X POST http://localhost:4566/_robotocore/state/save -d '{"name":"my-state"}'
   # Load after restart
   curl -X POST http://localhost:4566/_robotocore/state/load -d '{"name":"my-state"}'
   ```

2. **Port number**: Robotocore uses 4566 (not 4569 or 9090). Update all endpoint URLs.

3. **Path-style is default**: Both path-style (`http://localhost:4566/bucket/key`) and virtual-hosted-style (`http://bucket.s3.localhost:4566/key`) work. Most SDKs default to path-style for custom endpoints.

4. **No `initialBuckets` config**: Create buckets programmatically in test setup or via a startup script.

5. **No HTTPS port**: Robotocore serves HTTP on 4566. For HTTPS, place a TLS-terminating proxy in front.

6. **Credentials**: Any syntactically valid AWS credentials are accepted. No authentication enforcement by default. The account ID `123456789012` is used for all requests.

---

## Docker Run Examples

```bash
# Basic
docker run -p 4566:4566 robotocore/robotocore

# With a custom host port (drop-in for s3rver)
docker run -p 4569:4566 robotocore/robotocore

# With a custom host port (drop-in for S3Mock)
docker run -p 9090:4566 robotocore/robotocore

# Docker Compose
# services:
#   robotocore:
#     image: robotocore/robotocore
#     ports:
#       - "4566:4566"
```
