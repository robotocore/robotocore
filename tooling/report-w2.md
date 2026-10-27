# W2 — Golden-test harness

## Files created

- `scripts/harness/golden_test.py` — primary replay + diff tool
- `scripts/harness/botocore_check.py` — end-to-end boto3 smoke test

## Usage

### golden_test.py

```bash
# Replay all baseline ops against the live Python server, with setup/teardown:
.venv/bin/python scripts/harness/golden_test.py --service all --setup --teardown \
    --out /tmp/golden/results-python.json

# Replay only SQS ops:
.venv/bin/python scripts/harness/golden_test.py --service sqs --setup --teardown \
    --endpoint http://localhost:4566

# Diff two result files (exit 0 iff all ops match on status + normalized body):
.venv/bin/python scripts/harness/golden_test.py --diff \
    /tmp/golden/results-python.json /tmp/golden/results-rust.json
```

### botocore_check.py

```bash
# Against the live Python server (default):
.venv/bin/python scripts/harness/botocore_check.py

# Against the Rust server once it serves SQS/S3/STS:
.venv/bin/python scripts/harness/botocore_check.py --endpoint http://localhost:9000
```

## How golden_test.py works

1. Loads the baseline JSON (default `/tmp/golden/baseline.json`).
2. For each entry: drops volatile request headers, re-signs with botocore
   SigV4Auth (service + region parsed from the recorded Authorization header),
   rewrites resource names to a per-run unique suffix (`golden-<4hex>`),
   and sends the request to the target endpoint.
3. Records status code, response body (parsed JSON or raw text), and
   response headers (minus volatile ones) into the output file.
4. `--setup` creates the queue/bucket via boto3 before replay;
   `--teardown` deletes them after.

## Normalization lists (module constants — extend here)

| Constant | Purpose |
|---|---|
| `VOLATILE_REQUEST_HEADERS` | Dropped on replay; re-added by re-signing |
| `VOLATILE_RESPONSE_HEADERS` | Dropped from the recorded result |
| `VOLATILE_BODY_FIELDS` | JSON fields stripped before diff comparison |

### `VOLATILE_BODY_FIELDS` detail

```python
{
    "*":   ["RequestId", "x-amz-request-id"],
    "sqs": ["ReceiptHandle", "MessageId", "SentTimestamp",
            "ApproximateFirstReceiveTimestamp", "MD5OfBody", "MD5OfMessageBody"],
    "s3":  ["ETag", "LastModified", "ChecksumCRC32", "ChecksumMode",
            "ChecksumAlgorithm", "Metadata"],
}
```

For SQS `Messages[*]` entries, the SQS list is applied per-message.
Error bodies (`__error__`) are compared by error code only (messages vary).

## Results summary (2026-08-24)

### golden_test.py --service all --setup --teardown

```
replaying 9 ops against http://localhost:4566 (bucket=golden-b88d, queue=golden-b88d)
setup: {'s3_bucket': 'golden-b88d', 'sqs_queue': 'golden-b88d'}
teardown: {'s3_bucket': 'golden-b88d', 'sqs_queue': 'golden-b88d'}
note: 1 ops skipped (no recorded HTTP request): ['get_access_key_info']
PASS: 8/9 ops replayed, 1 skipped
```

All replayed ops returned status 200 (or the expected 404 for
`get_object` on a missing key). `get_access_key_info` is skipped because
the baseline recorded a client-side `ParamValidationError` (no HTTP request
was sent).

### botocore_check.py

```
PASS  sqs.create_queue
PASS  sqs.get_queue_url
PASS  sqs.send_message x3
PASS  sqs.receive_message
PASS  sqs.change_message_visibility
PASS  sqs.delete_message
PASS  sqs.get_queue_attributes
PASS  sqs.purge_queue
PASS  sqs.list_queues
PASS  sqs.delete_queue
PASS  sts.get_caller_identity
PASS  s3.create_bucket
PASS  s3.put_object
PASS  s3.get_object
PASS  s3.list_objects_v2
PASS  s3.delete_object
PASS  s3.delete_bucket

PASS: 17/17 steps
```

## Known limitations

- `put_object` body in the baseline is a `BytesIO` placeholder; the
  harness reconstructs it from the known capture plan
  (`b"hello golden world"` in `capture.py`).
- `get_access_key_info` cannot be replayed (client-side param validation).
- The diff normalizer strips volatile fields but does not handle nested
  S3 `Contents[*]` entries yet (those are compared as-is; the S3 volatile
  list covers the top-level fields).
