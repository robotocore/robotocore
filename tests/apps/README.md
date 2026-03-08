# Real-World Application Integration Tests

These tests simulate real applications using AWS services. They are **completely isolated** from robotocore internals -- the only dependencies are `boto3`, `pytest`, and the Python standard library.

## What's tested

| Test file | Services | Pattern |
|-----------|----------|---------|
| `test_file_processing_app.py` | S3, DynamoDB | File upload, metadata storage, versioning, multipart |
| `test_message_queue_app.py` | SQS | Producer/consumer, FIFO ordering, DLQ, batch ops |
| `test_event_driven_app.py` | EventBridge, SNS, SQS | Event routing, pattern filtering, fan-out |
| `test_secrets_rotation_app.py` | Secrets Manager | Create/read/update, versions, delete/restore, tags |
| `test_serverless_api_app.py` | DynamoDB, Lambda, IAM, API Gateway, Step Functions | CRUD, invoke, roles, REST API, state machines |

## Running

Against robotocore or LocalStack:

```bash
AWS_ENDPOINT_URL=http://localhost:4566 pytest tests/apps/ -v
```

Against real AWS (with credentials configured):

```bash
unset AWS_ENDPOINT_URL
pytest tests/apps/ -v
```

With explicit credentials:

```bash
AWS_ENDPOINT_URL=http://localhost:4566 \
AWS_ACCESS_KEY_ID=testing \
AWS_SECRET_ACCESS_KEY=testing \
AWS_DEFAULT_REGION=us-east-1 \
pytest tests/apps/ -v
```

## Design principles

- **Zero internal imports**: No `robotocore`, `localstack`, or `moto` imports. These tests don't know what backend they're running against.
- **Realistic naming**: Resource names use app-domain language (`user-uploads-bucket`, `order-processing-queue`) with UUID suffixes to avoid collisions.
- **Self-cleaning**: Every test cleans up its resources via fixtures.
- **Portable**: Works against robotocore, LocalStack, or real AWS without modification.
