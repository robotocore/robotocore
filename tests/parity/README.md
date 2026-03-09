# AWS Parity Tests

End-to-end tests that prove robotocore handles common multi-service workflows correctly.

## What these tests prove

Each test exercises a real AWS multi-service integration pattern -- the kind of thing developers use an AWS emulator for in their day-to-day work. If these pass against robotocore, it means robotocore is a viable drop-in replacement for that workflow.

| Test file | Services | Pattern | Derived from |
|---|---|---|---|
| `test_notes_app.py` | API Gateway + Lambda + DynamoDB | REST API CRUD | note-taking scenario |
| `test_sqs_lambda.py` | SQS + Lambda + DynamoDB | Event source mapping | Common pattern |
| `test_s3_lifecycle.py` | S3 | CRUD, presigned URLs, versioning, multipart | S3 integration tests |
| `test_dynamodb_streams.py` | DynamoDB Streams + Lambda + DynamoDB | Stream -> Lambda ESM | bookstore scenario |
| `test_kinesis_lambda.py` | Kinesis + Lambda + DynamoDB | Stream -> Lambda ESM | kinesis/firehose scenario |

## How to run

```bash
# Against robotocore (default, localhost:4566)
make start
AWS_ENDPOINT_URL=http://localhost:4566 uv run pytest tests/parity/ -v

# Against real AWS (requires credentials)
unset AWS_ENDPOINT_URL
uv run pytest tests/parity/ -v
```

## Design principles

- **No internal imports** -- tests are pure boto3 + requests
- **Self-contained** -- each test creates its own resources and cleans up in `finally` blocks
- **Portable** -- works against any AWS-compatible endpoint via `AWS_ENDPOINT_URL`
- **Meaningful assertions** -- every test verifies actual behavior, not just status codes

## CI

These tests run in the `parity.yml` GitHub Actions workflow against robotocore.
