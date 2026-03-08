# LocalStack Parity Tests

End-to-end tests that prove robotocore can replace LocalStack for common multi-service workflows.

## What these tests prove

Each test exercises a real AWS multi-service integration pattern -- the kind of thing developers use LocalStack for in their day-to-day work. If these pass against robotocore, it means robotocore is a viable drop-in replacement for that workflow.

| Test file | Services | Pattern | Derived from |
|---|---|---|---|
| `test_notes_app.py` | API Gateway + Lambda + DynamoDB | REST API CRUD | [note_taking scenario](../../tests/aws/scenario/note_taking/) |
| `test_sqs_lambda.py` | SQS + Lambda + DynamoDB | Event source mapping | Common pattern |
| `test_s3_lifecycle.py` | S3 | CRUD, presigned URLs, versioning, multipart | S3 integration tests |
| `test_dynamodb_streams.py` | DynamoDB Streams + Lambda + DynamoDB | Stream -> Lambda ESM | [bookstore scenario](../../tests/aws/scenario/bookstore/) |
| `test_kinesis_lambda.py` | Kinesis + Lambda + DynamoDB | Stream -> Lambda ESM | [kinesis_firehose scenario](../../tests/aws/scenario/kinesis_firehose/) |

## How to run

```bash
# Against robotocore (default, localhost:4566)
make start
AWS_ENDPOINT_URL=http://localhost:4566 uv run pytest tests/localstack_parity/ -v

# Against LocalStack
localstack start -d && localstack wait
AWS_ENDPOINT_URL=http://localhost:4566 uv run pytest tests/localstack_parity/ -v

# Against real AWS (requires credentials)
unset AWS_ENDPOINT_URL
uv run pytest tests/localstack_parity/ -v
```

## Design principles

- **No imports from robotocore or localstack** -- tests are pure boto3 + requests
- **Self-contained** -- each test creates its own resources and cleans up in `finally` blocks
- **Portable** -- works against any AWS-compatible endpoint via `AWS_ENDPOINT_URL`
- **Meaningful assertions** -- every test verifies actual behavior, not just status codes

## CI

These tests run in the `localstack-parity.yml` GitHub Actions workflow against both robotocore and LocalStack (for comparison). The LocalStack job uses `continue-on-error: true` so it doesn't block merges.
